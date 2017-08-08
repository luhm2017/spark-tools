package lakala.utils

import redis.clients.jedis.JedisCluster

/**
  * Created by longxiaolei on 2017/7/10.
  */
object TestRedis {
  def main(args: Array[String]) {
    val jedis: JedisCluster = EnvUtil.jedisCluster("test1")
    jedis.publish("creditloan_order_score_result_channel","test11111")
    if(jedis != null && jedis.exists("test") ){
      println("test redis")
      println(jedis.get("creditloan_order_score_result_channel"))
    }
  }
}

