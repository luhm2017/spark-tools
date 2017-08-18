package lakala.graphx

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.commons.lang.StringUtils

import scala.collection.mutable.ListBuffer
import lakala.graphx.util.UtilsTools._

/**
  * Created by pattrick on 2017/8/16.
  */
object GraphxCalculate {

  //定义边属性
  case class EdgeArr(srcValue: String, dstValue: String, srcType: String, dstType: String)

  val conf = new SparkConf().setAppName("GraphxCalculate")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //设置序列化为kryo
  val sc = new SparkContext(conf)
  val hc = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println("请输入参数：database、table")
      System.exit(0)
    }
    val database = args(0)
    val table = args(1)
    //val edgeRDD = generateEdgRDD(database,table)
    //runNHopResult(edgeRDD)

  }

  //根据hive表构建Edge
  /*def generateEdgRDD1(database:String,table:String): RDD[Edge[EdgeArr]] ={
    //提取hive数据，数据类型全部转String处理
    val rawData = hc.sql(s"select * from $database.$table")
    //提取hive数据，数据类型全部转String处理
    val edgeRDD = rawData.map{
      line =>
        val orderId = line.getString(0)
        val edgeList = ListBuffer[Edge[EdgeArr]]()
        //遍历当前line，组装Edge
        for(i<- 2 until line.size){
          val srcVertex = line.getString(i)
          if (!line.isNullAt(i) && StringUtils.isNotBlank(srcVertex)) {
            edgeList +=(Edge(hashId(srcVertex), hashId(orderId), EdgeArr(srcVertex, orderId, s"$i", "1")))
          }
        }
        //返回edge数组
        edgeList
    }
    //使用hashId方法
    edgeRDD
  }*/

  //构建Edge
  def generateEdgRDD(database:String,table:String): RDD[Edge[EdgeArr]] = {
    //提取hive数据，数据类型全部转String处理
    val rawData = hc.sql(s"select * from $database.$table")
    //按partition遍历处理
    val edgeRDD = rawData.mapPartitions(
      lines => lines.map {
        line =>
          val orderId = line.getString(0)
          val edgeList = ListBuffer[Edge[EdgeArr]]()
          //遍历当前line，组装Edge
          for(i<- 2 until line.size){
            val srcVertex = line.getString(i)
            if (!line.isNullAt(i) && StringUtils.isNotBlank(srcVertex)) {
              edgeList.+=(Edge(hashId(srcVertex), hashId(orderId), EdgeArr(srcVertex, orderId, s"$i", "1")))
            }
          }
          //返回edge数组
          edgeList
      //flatten操作用于合并数组，用于将嵌套List组合成一个新的List
      }.flatten)
    //返回Edge素组
    edgeRDD
  }


}

  //计算N度关联数据
  /*def runNHopResult(edges:RDD[Edge[EdgeArr]]): Unit = {
    //根据edge构造图
    var graph = Graph.fromEdges(edges,"")

    println("========================")
    var preG: Graph[String, EdgeArr] = null
    var iterCount = 0
    while (iterCount < args(1).toInt) {
      println(s"iteration $iterCount start .....")
      preG = g
      //      if ((iterCount + 1) % 2 == 0) {
      //        g.checkpoint()
      //        println(g.numEdges)
      //      }
      var tmpG = g.aggregateMessages[String](sendMsg, merageMsg).cache()
      print("下一次迭代要发送的messages:" + tmpG.filter(v => StringUtils.isNotBlank(v._2)).take(10).mkString("\n"))
      g = g.outerJoinVertices(tmpG) { (_, _, updateAtt) => updateAtt.getOrElse("") }

      g.cache() //新的graph cache起来，下一次迭代使用
      //      print(g.vertices.take(10).mkString("\n"))
      tmpG.unpersist(blocking = false)
      preG.vertices.unpersist(blocking = false)
      preG.edges.unpersist(blocking = false)
      println(s"iteration $iterCount end .....")
      iterCount += 1
    }
    //************************
    val result = g.vertices
      .mapPartitions(vs => vs.filter(v => StringUtils.isNotBlank(v._2)).flatMap(v => v._2.split("-->")).filter(_.split("->").length == args(1).toInt + 1))
    result.saveAsTextFile(args(2))
  }*/


}
