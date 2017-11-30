package lakala.neo4j.utils

import java.util

import lakala.neo4j.exportData.StreamingConstant
import redis.clients.jedis.{HostAndPort, JedisCluster}
import lakala.neo4j.utils.UtilsTools.properties
import scala.collection.JavaConversions

/**
  * Created by Administrator on 2017/6/29 0029.
  */
object RedisUtils {
  private var cluster: JedisCluster = _
  private val properies = properties(StreamingConstant.CONFIG)

  def jedisCluster(): JedisCluster = {
    if (cluster == null) {
      synchronized {
        if (cluster == null) {
          val cluseterNodesSet = for (ipAndPort <- properies.getProperty("redisIp").split(",")) yield
            new HostAndPort(ipAndPort.split(":")(0).trim, (ipAndPort.split(":")(1).trim).toInt)
          cluster = new JedisCluster(JavaConversions.setAsJavaSet[HostAndPort](cluseterNodesSet.toSet))
        }
      }
    }
    cluster
  }
}
