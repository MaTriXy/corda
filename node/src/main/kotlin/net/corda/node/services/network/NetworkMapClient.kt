package net.corda.node.services.network

import com.fasterxml.jackson.databind.ObjectMapper
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.SignedData
import net.corda.core.node.NodeInfo
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.utilities.loggerFor
import net.corda.core.utilities.minutes
import java.net.HttpURLConnection
import java.net.URL

interface NetworkMapClient {
    companion object {
        // This default value is used in case the cache timeout is not included in the HTTP header.
        // TODO : Make this configurable?
        val defaultNetworkMapTimeout: Long = 1.minutes.toMillis()
        val logger = loggerFor<NetworkMapClient>()
    }
    /**
     *  Publish node info to network map service.
     */
    fun publish(signedNodeInfo: SignedData<NodeInfo>)

    /**
     *  Retrieve [NetworkMap] from the network map service containing list of node info hashes and network parameter hash.
     */
    // TODO: Use NetworkMap object when available.
    fun getNetworkMap(): NetworkMapResponse

    /**
     *  Retrieve [NodeInfo] from network map service using the node info hash.
     */
    fun getNodeInfo(nodeInfoHash: SecureHash): NodeInfo?

    fun myPublicHostname(): String

    // TODO: Implement getNetworkParameter when its available.
    //fun getNetworkParameter(networkParameterHash: SecureHash): NetworkParameter
}

class HTTPNetworkMapClient(compatibilityZoneURL: URL) : NetworkMapClient {
    private val networkMapUrl = URL("$compatibilityZoneURL/network-map")

    override fun publish(signedNodeInfo: SignedData<NodeInfo>) {
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

    override fun getNetworkMap(): NetworkMapResponse {
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

    override fun getNodeInfo(nodeInfoHash: SecureHash): NodeInfo? {
        val nodeInfoURL = URL("$networkMapUrl/$nodeInfoHash")
        val conn = nodeInfoURL.openConnection() as HttpURLConnection

        return when (conn.responseCode) {
            HttpURLConnection.HTTP_OK -> conn.inputStream.readBytes().deserialize()
            HttpURLConnection.HTTP_NOT_FOUND -> null
            else -> throw IllegalArgumentException("Unexpected response code ${conn.responseCode}, response error message: '${conn.errorStream.bufferedReader().readLines()}'")
        }
    }

    override fun myPublicHostname(): String {
        val nodeInfoURL = URL("$networkMapUrl/my-hostname")
        val conn = nodeInfoURL.openConnection() as HttpURLConnection

        return when (conn.responseCode) {
            HttpURLConnection.HTTP_OK -> conn.inputStream.readBytes().deserialize()
            else -> throw IllegalArgumentException("Unexpected response code ${conn.responseCode}, response error message: '${conn.errorStream.bufferedReader().readLines()}'")
        }
    }
}

data class NetworkMapResponse(val networkMap: List<SecureHash>, val cacheMaxAge: Long)