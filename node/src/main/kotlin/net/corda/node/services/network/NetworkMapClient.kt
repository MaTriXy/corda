package net.corda.node.services.network

import com.fasterxml.jackson.databind.ObjectMapper
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.SignedData
import net.corda.core.internal.openHTTPConnection
import net.corda.core.node.NodeInfo
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.utilities.loggerFor
import net.corda.core.utilities.minutes
import net.corda.node.services.api.ServiceHubInternal
import net.corda.node.utilities.NamedThreadFactory
import java.net.HttpURLConnection
import java.net.URL
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class NetworkMapClient(compatibilityZoneURL: URL) {
    companion object {
        // This default value is used in case the cache timeout is not included in the HTTP header.
        val logger = loggerFor<NetworkMapClient>()
    }

    private val networkMapUrl = URL("$compatibilityZoneURL/network-map")

    fun publish(signedNodeInfo: SignedData<NodeInfo>) {
        val publishURL = URL("$networkMapUrl/publish")
        val conn = publishURL.openHTTPConnection()
        conn.doOutput = true
        conn.requestMethod = "POST"
        conn.setRequestProperty("Content-Type", "application/octet-stream")
        conn.outputStream.write(signedNodeInfo.serialize().bytes)

        // This will throw IOException if the response code is not HTTP 200.
        conn.inputStream
    }

    fun getNetworkMap(): NetworkMapResponse {
        val conn = networkMapUrl.openHTTPConnection()
        val response = conn.inputStream.bufferedReader().use { it.readLine() }
        val networkMap = ObjectMapper().readValue(response, List::class.java).map { SecureHash.parse(it.toString()) }
        val timeout = conn.headerFields["Cache-Control"]?.single { it.startsWith("max-age=") }?.removePrefix("max-age=")?.toLong() ?: throw IllegalArgumentException("Network map cache timeout is missing from the respond header.")
        return NetworkMapResponse(networkMap, timeout)
    }

    fun getNodeInfo(nodeInfoHash: SecureHash): NodeInfo? {
        val conn = URL("$networkMapUrl/$nodeInfoHash").openHTTPConnection()
        return if (conn.responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
            null
        } else {
            conn.inputStream.readBytes().deserialize()
        }
    }

    fun myPublicHostname(): String {
        val conn = URL("$networkMapUrl/my-hostname").openHTTPConnection()
        return conn.inputStream.bufferedReader().readLine()
    }
}

data class NetworkMapResponse(val networkMap: List<SecureHash>, val cacheMaxAge: Long)

object NetworkMapUpdater {
    private val logger = loggerFor<NetworkMapUpdater>()
    private val retryInterval = 1.minutes

    // This method store
    fun updateNodeInfo(serviceHub: ServiceHubInternal, networkMapClient: NetworkMapClient?) {
        val newInfo = serviceHub.myInfo
        val oldInfo = serviceHub.networkMapCache.getNodeByLegalIdentity(serviceHub.myInfo.legalIdentities.first())
        // Compare node info without timestamp.
        if (newInfo.copy(serial = 0L) != oldInfo?.copy(serial = 0L)) {
            // Only publish and write to disk if there are changes to the node info.
            val serialisedNodeInfo = newInfo.serialize()
            val signature = serviceHub.keyManagementService.sign(serialisedNodeInfo.bytes, newInfo.legalIdentities.first().owningKey)
            val signedNodeInfo = SignedData(serialisedNodeInfo, signature)
            NodeInfoWatcher.saveToFile(serviceHub.configuration.baseDirectory, signedNodeInfo)
            if (networkMapClient != null) {
                tryPublishNodeInfoAsync(signedNodeInfo, networkMapClient)
            }
        }
    }

    fun subscriptToNetworkMap(serviceHub: ServiceHubInternal, networkMapClient: NetworkMapClient?) {
        // Subscribe to file based networkMap
        val fileWatcher = NodeInfoWatcher(serviceHub.configuration.baseDirectory, Duration.ofMillis(serviceHub.configuration.additionalNodeInfoPollingFrequencyMsec))
        fileWatcher.nodeInfoUpdates().subscribe { node -> serviceHub.networkMapCache.addNode(node) }
        // Subscribe to remote network map if configured.
        if (networkMapClient != null) {
            val executor = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory("Network Map Update Thread", Executors.defaultThreadFactory()))
            val task = object : Runnable {
                override fun run() {
                    try {
                        val (networkMap, cacheTimeout) = networkMapClient.getNetworkMap()
                        networkMap.subtract(serviceHub.networkMapCache.allNodeHashes).mapNotNull {
                            // Download new node info from network map
                            networkMapClient.getNodeInfo(it)
                        }.forEach {
                            // Add new node info to the network map cache, these could be new node info or modification of node info for existing nodes.
                            serviceHub.networkMapCache.addNode(it)
                        }

                        // Remove node info from network map.
                        serviceHub.networkMapCache.allNodeHashes.subtract(networkMap)
                                .mapNotNull { serviceHub.networkMapCache.getNodeByHash(it) }
                                .forEach { serviceHub.networkMapCache.removeNode(it) }

                        // Schedule the next update.
                        executor.schedule(this, cacheTimeout, TimeUnit.MILLISECONDS)
                    } catch (t: Throwable) {
                        logger.warn("Error encountered while updating network map, will retry in $retryInterval", t)
                        executor.schedule(this, retryInterval.toMillis(), TimeUnit.MILLISECONDS)
                    }
                }
            }
            executor.submit(task) // The check may be expensive, so always run it in the background even the first time.
        }
    }

    private fun tryPublishNodeInfoAsync(signedNodeInfo: SignedData<NodeInfo>, networkMapClient: NetworkMapClient) {
        val executor = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory("Node Info Publish Thread", Executors.defaultThreadFactory()))
        val task = object : Runnable {
            override fun run() {
                try {
                    networkMapClient.publish(signedNodeInfo)
                } catch (t: Throwable) {
                    logger.warn("Error encountered while publishing node info, will retry in $retryInterval.", t)
                    executor.schedule(this, retryInterval.toMillis(), TimeUnit.MILLISECONDS)
                }
            }
        }
        executor.submit(task)
    }
}