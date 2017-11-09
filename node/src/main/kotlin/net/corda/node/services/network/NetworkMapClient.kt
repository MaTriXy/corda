package net.corda.node.services.network

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.util.concurrent.MoreExecutors
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.SignedData
import net.corda.core.internal.openHttpConnection
import net.corda.core.node.NodeInfo
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.utilities.loggerFor
import net.corda.core.utilities.minutes
import net.corda.core.utilities.seconds
import net.corda.node.services.api.NetworkMapCacheInternal
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.utilities.NamedThreadFactory
import okhttp3.CacheControl
import okhttp3.Headers
import rx.Subscription
import java.io.BufferedReader
import java.io.Closeable
import java.io.IOException
import java.net.HttpURLConnection
import java.net.URL
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class NetworkMapClient(compatibilityZoneURL: URL) {
    companion object {
        val logger = loggerFor<NetworkMapClient>()
    }

    private val networkMapUrl = URL("$compatibilityZoneURL/network-map")

    fun publish(signedNodeInfo: SignedData<NodeInfo>) {
        val publishURL = URL("$networkMapUrl/publish")
        val conn = publishURL.openHttpConnection()
        conn.doOutput = true
        conn.requestMethod = "POST"
        conn.setRequestProperty("Content-Type", "application/octet-stream")
        conn.outputStream.write(signedNodeInfo.serialize().bytes)

        // This will throw IOException if the response code is not HTTP 200.
        if (conn.responseCode != HttpURLConnection.HTTP_OK) {
            throw IOException(conn.errorStream.bufferedReader().use(BufferedReader::readLine))
        }
    }

    fun getNetworkMap(): NetworkMapResponse {
        val conn = networkMapUrl.openHttpConnection()
        val response = conn.inputStream.bufferedReader().use(BufferedReader::readLine)
        val networkMap = ObjectMapper().readValue(response, List::class.java).map { SecureHash.parse(it.toString()) }
        val timeout = CacheControl.parse(Headers.of(conn.headerFields.filterKeys { it != null }.mapValues { it.value.first() })).maxAgeSeconds().seconds
        return NetworkMapResponse(networkMap, timeout)
    }

    fun getNodeInfo(nodeInfoHash: SecureHash): NodeInfo? {
        val conn = URL("$networkMapUrl/$nodeInfoHash").openHttpConnection()
        return if (conn.responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
            null
        } else {
            conn.inputStream.use { it.readBytes() }.deserialize()
        }
    }

    fun myPublicHostname(): String {
        val conn = URL("$networkMapUrl/my-hostname").openHttpConnection()
        return conn.inputStream.bufferedReader().use(BufferedReader::readLine)
    }
}

data class NetworkMapResponse(val networkMap: List<SecureHash>, val cacheMaxAge: Duration)

class NetworkMapUpdater(private val config: NodeConfiguration,
                        private val networkMapCache: NetworkMapCacheInternal,
                        private val networkMapClient: NetworkMapClient?) : Closeable {
    companion object {
        private val logger = loggerFor<NetworkMapUpdater>()
        private val retryInterval = 1.minutes
    }

    private val executor = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory("Network Map Updater Thread", Executors.defaultThreadFactory()))
    private var fileWatcherSubscription: Subscription? = null

    override fun close() {
        fileWatcherSubscription?.unsubscribe()
        MoreExecutors.shutdownAndAwaitTermination(executor, 50, TimeUnit.SECONDS)
    }

    fun updateNodeInfo(newInfo: NodeInfo, signNodeInfo: (NodeInfo) -> SignedData<NodeInfo>) {
        val oldInfo = networkMapCache.getNodeByLegalIdentity(newInfo.legalIdentities.first())
        // Compare node info without timestamp.
        if (newInfo.copy(serial = 0L) == oldInfo?.copy(serial = 0L)) return

        // Only publish and write to disk if there are changes to the node info.
        val signedNodeInfo = signNodeInfo(newInfo)
        NodeInfoWatcher.saveToFile(config.baseDirectory, signedNodeInfo)
        if (networkMapClient != null) {
            tryPublishNodeInfoAsync(signedNodeInfo, networkMapClient)
        }
    }

    fun subscribeToNetworkMap() {
        // Subscribe to file based networkMap
        val fileWatcher = NodeInfoWatcher(config.baseDirectory, Duration.ofMillis(config.additionalNodeInfoPollingFrequencyMsec))
        fileWatcherSubscription?.unsubscribe()
        fileWatcherSubscription = fileWatcher.nodeInfoUpdates().subscribe(networkMapCache::addNode)

        if (networkMapClient == null) return
        // Subscribe to remote network map if configured.
        val task = object : Runnable {
            override fun run() {
                val nextScheduleDelay = try {
                    val (networkMap, cacheTimeout) = networkMapClient.getNetworkMap()
                    val currentNodeHashes = networkMapCache.allNodeHashes
                    (networkMap - currentNodeHashes).mapNotNull {
                        // Download new node info from network map
                        networkMapClient.getNodeInfo(it)
                    }.forEach {
                        // Add new node info to the network map cache, these could be new node info or modification of node info for existing nodes.
                        networkMapCache.addNode(it)
                    }
                    // Remove node info from network map.
                    (currentNodeHashes - networkMap)
                            .mapNotNull(networkMapCache::getNodeByHash)
                            .forEach(networkMapCache::removeNode)

                    cacheTimeout
                } catch (t: Throwable) {
                    logger.warn("Error encountered while updating network map, will retry in $retryInterval", t)
                    retryInterval
                }
                // Schedule the next update.
                executor.schedule(this, nextScheduleDelay.toMillis(), TimeUnit.MILLISECONDS)
            }
        }
        executor.submit(task) // The check may be expensive, so always run it in the background even the first time.
    }

    private fun tryPublishNodeInfoAsync(signedNodeInfo: SignedData<NodeInfo>, networkMapClient: NetworkMapClient) {
        val task = object : Runnable {
            override fun run() {
                try {
                    networkMapClient.publish(signedNodeInfo)
                } catch (t: Throwable) {
                    logger.warn("Error encountered while publishing node info, will retry in $retryInterval.", t)
                    // TODO: Exponential backoff?
                    executor.schedule(this, retryInterval.toMillis(), TimeUnit.MILLISECONDS)
                }
            }
        }
        executor.submit(task)
    }
}