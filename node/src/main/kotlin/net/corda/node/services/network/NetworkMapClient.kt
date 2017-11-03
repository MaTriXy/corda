package net.corda.node.services.network

import com.fasterxml.jackson.databind.ObjectMapper
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.SignedData
import net.corda.core.node.NodeInfo
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.utilities.loggerFor
import net.corda.core.utilities.minutes
import net.corda.node.services.api.ServiceHubInternal
import java.net.HttpURLConnection
import java.net.URL
import java.time.Duration
import kotlin.concurrent.thread

class NetworkMapClient(compatibilityZoneURL: URL) {
    companion object {
        // This default value is used in case the cache timeout is not included in the HTTP header.
        // TODO : Make this configurable?
        val defaultNetworkMapTimeout: Long = 1.minutes.toMillis()
        val logger = loggerFor<NetworkMapClient>()
    }

    private val networkMapUrl = URL("$compatibilityZoneURL/network-map")

    fun publish(signedNodeInfo: SignedData<NodeInfo>) {
        val publishURL = URL("$networkMapUrl/publish")
        val conn = publishURL.openConnection() as HttpURLConnection
        conn.doOutput = true
        conn.requestMethod = "POST"
        conn.setRequestProperty("Content-Type", "application/octet-stream")
        conn.outputStream.write(signedNodeInfo.serialize().bytes)
        when (conn.responseCode) {
            HttpURLConnection.HTTP_OK -> return
            HttpURLConnection.HTTP_UNAUTHORIZED -> throw IllegalArgumentException(conn.errorStream.bufferedReader().readLine())
            else -> throw IllegalArgumentException("Unexpected response code ${conn.responseCode}, response error message: '${conn.errorStream.bufferedReader().readLines()}'")
        }
    }

    fun getNetworkMap(): NetworkMapResponse {
        val conn = networkMapUrl.openConnection() as HttpURLConnection
        val networkMap = when (conn.responseCode) {
            HttpURLConnection.HTTP_OK -> {
                val response = conn.inputStream.bufferedReader().use { it.readLine() }
                ObjectMapper().readValue(response, List::class.java).map { SecureHash.parse(it.toString()) }
            }
            else -> throw IllegalArgumentException("Unexpected response code ${conn.responseCode}, response error message: '${conn.errorStream.bufferedReader().readLines()}'")
        }
        val timeout = conn.headerFields["Cache-Control"]?.find { it.startsWith("max-age=") }?.removePrefix("max-age=")?.toLong()
        return NetworkMapResponse(networkMap, timeout ?: NetworkMapClient.defaultNetworkMapTimeout)
    }

    fun getNodeInfo(nodeInfoHash: SecureHash): NodeInfo? {
        val nodeInfoURL = URL("$networkMapUrl/$nodeInfoHash")
        val conn = nodeInfoURL.openConnection() as HttpURLConnection

        return when (conn.responseCode) {
            HttpURLConnection.HTTP_OK -> conn.inputStream.readBytes().deserialize()
            HttpURLConnection.HTTP_NOT_FOUND -> null
            else -> throw IllegalArgumentException("Unexpected response code ${conn.responseCode}, response error message: '${conn.errorStream.bufferedReader().readLines()}'")
        }
    }

    fun myPublicHostname(): String {
        val nodeInfoURL = URL("$networkMapUrl/my-hostname")
        val conn = nodeInfoURL.openConnection() as HttpURLConnection

        return when (conn.responseCode) {
            HttpURLConnection.HTTP_OK -> conn.inputStream.readBytes().deserialize()
            else -> throw IllegalArgumentException("Unexpected response code ${conn.responseCode}, response error message: '${conn.errorStream.bufferedReader().readLines()}'")
        }
    }
}

data class NetworkMapResponse(val networkMap: List<SecureHash>, val cacheMaxAge: Long)

class NetworkMapUpdater(private val serviceHub: ServiceHubInternal, private val networkMapClient: NetworkMapClient?) {
    companion object {
        val logger = loggerFor<NetworkMapUpdater>()
    }

    fun startUpdate() {
        val newInfo = serviceHub.myInfo
        val oldInfo = serviceHub.networkMapCache.getNodeByLegalIdentity(serviceHub.myInfo.legalIdentities.first())
        // TODO: Make this configurable?
        val defaultRetryInterval = 1.minutes
        // Compare node info without timestamp.
        if (newInfo.copy(serial = 0L) != oldInfo?.copy(serial = 0L)) {
            // Only publish and write to disk if there are changes to the node info.
            // This method is non-blocking.
            tryPublish(defaultRetryInterval, newInfo)
        }
        val fileWatcher = NodeInfoWatcher(serviceHub.configuration.baseDirectory, serviceHub.configuration.additionalNodeInfoPollingFrequencyMsec)
        fileWatcher.nodeInfoUpdates().subscribe { node -> serviceHub.networkMapCache.addNode(node) }

        // Poll network map for updates periodically, if network map URL has been configured.
        maybeSubscriptToNetworkMap(defaultRetryInterval)
    }

    private fun tryPublish(retryInterval: Duration, nodeInfo: NodeInfo) {
        thread(name = "Node Info Publish Thread") {
            val serialisedNodeInfo = nodeInfo.serialize()
            val signature = serviceHub.keyManagementService.sign(serialisedNodeInfo.bytes, nodeInfo.legalIdentities.first().owningKey)
            val signedNodeInfo = SignedData(serialisedNodeInfo, signature)
            if (networkMapClient != null) {
                while (true) {
                    // Retry until successfully registered with the network map.
                    try {
                        networkMapClient.publish(signedNodeInfo)
                    } catch (e: IllegalArgumentException) {
                        logger.warn("Error encountered while publishing node info, will retry in $retryInterval.", e)
                        Thread.sleep(retryInterval.toMillis())
                    }
                }
            }
            NodeInfoWatcher.saveToFile(serviceHub.configuration.baseDirectory, signedNodeInfo)
        }
    }

    private fun maybeSubscriptToNetworkMap(retryInterval: Duration) {
        if (networkMapClient != null) {
            thread(name = "Network Map Update Thread") {
                while (true) {
                    try {
                        val (networkMap, cacheTimeout) = networkMapClient.getNetworkMap()
                        val currentNodeHashes = serviceHub.networkMapCache.allNodeHashes
                        val toBeAdded = networkMap.subtract(currentNodeHashes).mapNotNull {
                            // Download new node info from network map
                            networkMapClient.getNodeInfo(it)
                        }
                        val toBeRemoved = currentNodeHashes.subtract(networkMap).mapNotNull { serviceHub.networkMapCache.getNodeByHash(it) }
                        // Remove node info from network map.
                        toBeRemoved.forEach {
                            serviceHub.networkMapCache.removeNode(it)
                        }
                        // Add new node info to the network map cache.
                        toBeAdded.forEach {
                            serviceHub.networkMapCache.addNode(it)
                        }
                        Thread.sleep(cacheTimeout)
                    } catch (e: IllegalArgumentException) {
                        logger.warn("Error encountered while updating network map, will retry in $retryInterval", e)
                        Thread.sleep(retryInterval.toMillis())
                    }
                }
            }
        }
    }
}