package lakala.models.lklCardScore

import java.sql.{Connection, DriverManager, Statement}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/5/9 0009.
  */
object ExplortApplyData{
  private var conf: SparkConf = _
  private var sc: SparkContext = _
//  private val ip = "jdbc:neo4j:bolt:10.16.65.15:7687"
  private val username = "neo4j"
  private val password = "123456"


  def main(args: Array[String]): Unit = {
     val ip = s"jdbc:neo4j:bolt:${args(0)}:7687"
    setUP
    val modelRdd = sc.parallelize(List("ApplyInfo", "BankCard", "Device", "Mobile", "MobileIMEI", "Terminal", "Email"))

    val broadcastVar = sc.broadcast(List("applymymobile", "loanapply", "emergencymobile", "device", "bankcard", "identification", "email"))


    val rs = modelRdd.mapPartitions { models =>
      val con: Connection = DriverManager.getConnection(ip, username, password)
      val stmt: Statement = con.createStatement
      stmt.setQueryTimeout(120)
      models.map { model =>
        broadcastVar.value.map(relV => runQueryApplyByApplyLevel0(stmt, model, relV))
      }.flatten
    }.flatMap(k => k)
    rs.repartition(1).saveAsTextFile(args(1))

    closeDown
  }

  def setUP() = {
    //    conf = new SparkConf().setAppName("ExplortApplyData").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true").set("spark.neo4j.bolt.url", "bolt://10.16.65.15:7687")
    conf = new SparkConf().setMaster("local[4]").setAppName("ExplortApplyData")/*.set("spark.driver.allowMultipleContexts", "true").set("spark.neo4j.bolt.url", "bolt://192.168.0.33:7687")*/
    sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  }

  def closeDown() = {
    sc.stop()
  }


  def runQueryApplyByApplyLevel0(stmt: Statement, modelname: String, relation: String): List[String] = {
    val rs = stmt.executeQuery("match (n:" + modelname + " {type:'1'})-[r:" + relation + "]-(m:ApplyInfo) return n.content,r,m.orderno")
    val buff = new ArrayBuffer[String]()
    val it = while (rs.next) {
//      System.out.println("n.content:" + rs.getString("n.content"))
//      System.out.println("m.orderno:" + rs.getString("m.orderno"))
      System.out.println(s"${rs.getString("n.content")},$relation,${rs.getString("m.orderno")}")
      buff += s"${rs.getString("n.content")},$relation,${rs.getString("m.orderno")}"
    }
    buff.toList
  }

}
