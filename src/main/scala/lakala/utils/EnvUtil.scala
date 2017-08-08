package lakala.utils

import java.io.InputStream
import java.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import redis.clients.jedis.{HostAndPort, JedisCluster}

import scala.io.Source


object EnvUtil {
  private val is: InputStream = getClass.getClassLoader.getResourceAsStream("env.json")
  private val json: String = Source.fromInputStream(is).mkString
  private val envInfo: JSONObject = JSON.parseObject(json)

  def getComponetInfoObj(env: String, componetName: String) = {
    envInfo.getJSONObject(env).getJSONObject(componetName)
  }

  def getComponetInfoArray(env: String, componetName: String) = {
    envInfo.getJSONObject(env).getJSONArray(componetName)
  }

  private var cluster: JedisCluster = _
  /**
    * 获取redis的信息
    *
    * @param env 环境信息
    * @return
    */
  def jedisCluster(env: String): JedisCluster = {
    if (cluster == null) {
      synchronized {
        if (cluster == null) {
          val nodis: JSONArray = EnvUtil.getComponetInfoObj(env, "redis").getJSONArray("nodes")
          val size: Int = nodis.size()
          println("---------------------------"+size)
          if (size <= 0) {
            throw new RuntimeException("获取配置信息失败，请检查env.json文件")
          }
          val jedisClusterNodes: util.HashSet[HostAndPort] = new util.HashSet[HostAndPort](size)
          val it = nodis.iterator()
          while (it.hasNext) {
            val nodeObject = it.next().asInstanceOf[JSONObject]
            val hostAndPort: HostAndPort = new HostAndPort(nodeObject.getString("host"), nodeObject.getIntValue("port"))
            println("----------"+hostAndPort)
            jedisClusterNodes.add(hostAndPort)
            println("---------------------jedisClusterNodes.size()"+jedisClusterNodes.size())
          }
          cluster = new JedisCluster(jedisClusterNodes)
        }
      }
    }
    cluster
  }
}
