/*
package lakala.graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by administrator on 2017/5/18.
  * 计算N度关系
  */
object GraphNDegree {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("GraphNDegree")
    val sc = new SparkContext(sparkConf)

    //order vertexIds (orderId,age,value)
    val vertexArray = Array(
      (1L, ("order001", 28,0)),
      (2L, ("order002", 27,1)),
      (3L, ("order003", 30,1)),
      (4L, ("order004", 18,1)),
      (5L, ("order005", 32,0)),
      (6L, ("order006", 27,0)),
      (7L, ("order007", 30,1)),
      (8L,("order008",  30,0)),
      (9L,("order009",  29,1))
    )
    //order edge
    val edgeArray = Array(
      Edge(1L, 2L, "mobile"),
      Edge(1L, 2L, "email"),
      Edge(2L, 3L, "device"),
      Edge(3L, 4L, "email"),
      Edge(2L, 5L, "cid"),
      Edge(5L, 6L, "device"),
      Edge(1L, 7L, "device"),
      Edge(1L, 7L, "phone"),
      Edge(6L, 8L, "cid"),
      Edge(4L, 8L, "device"),
      Edge(8L, 9L, "email")
    )

    //数据结构，从hive读取
    /**
      * (1,2)
      * (1,7)
      * (1,2,3)
      * (1,2,5)
      * (1,2,3,4,8,9)
      * (1,2,5,6,8,9)
      * */

    //构建顶点
    val vertexRDD:RDD[(VertexId,(String,Int,Int))] = sc.parallelize(vertexArray)
    //构建边
    val edgeRDD:RDD[Edge[String]] = sc.parallelize(edgeArray)
    //rdd加载图数据
    val orderGraph = Graph(vertexRDD,edgeRDD)
    //关系深度
    val totalRounds: Int = 4 // total N round
    //出发源点
    val targetVerticeID: Long = 1 // target vertice
    //orderGraph.triplets.collect.foreach(println(_))

    //初始化读取统计的边权重，直接从hive读取
    var weightMap:Map[String,Double] = Map()
    weightMap += ("mobile" -> 0.5)
    weightMap += ("phone"->0.5)
    weightMap += ("device"->0.8)
    weightMap += ("email"->0.2)
    weightMap += ("mobile|device"->0.1)
    weightMap += ("mobile|cid"->0.1)
    weightMap += ("mobile|device|email"->0.1)
    weightMap += ("mobile|device|device"->0.1)
    weightMap += ("mobile|device|email|device"->0.1)
    weightMap += ("mobile|device|email|cid"->0.1)
    weightMap += ("mobile|device|email|device|email"->0.1)
    weightMap += ("mobile|device|email|cid|email"->0.1)

    // 初始化顶点属性(Map())
    val roundGraph = orderGraph.mapVertices((id, vd) => (vd._1,vd._2,vd._3))
    //roundGraph.triplets.collect.foreach(println(_))

    //first send msg to dstId，roundVertice为过滤出来的顶点关系对
    //传递边
    var roundVertices = roundGraph.aggregateMessages[(String,Int)](
      ctx => {
        //从源点出发
        if (targetVerticeID == ctx.srcId) {
          // send msg，分别传递关联边、dst顶点表现
          ctx.sendToDst(ctx.attr,ctx.dstAttr._3)
        }
      },
      //先聚合计算，然后再存储
      (a,b) => (weightMap.get(a._1) * a._2 + weightMap.get(b._1) * b._2 )
      //(a,b) => (a._1+b._1,a._2+b._2)
    )
    //roundVertices.collect.foreach(println(_))
    //roundVertices.map(row => "一度关联：边："+ row._2._1 +"，顶点："+row._1+"，其合同表现："+ row._2._2).collect.foreach(println(_))
    //计算一度关联的评分
    //val Score = roundVertices.map(row => "一度关联：边："+ row._2._1 +"，顶点："+row._1+"，其合同表现："+ row._2._2)

    for (i <- 2 to totalRounds) {
      val thisRoundGraph = roundGraph.joinVertices(roundVertices){
        //vid为顶点，data为old顶点属性，opt为join的顶点
       // (vid, data, outDeg) => outDeg._3
        (vid, data, opt) => opt
      }

      //thisRoundGraph.triplets.collect.foreach(println(_))
      //聚合消息
      roundVertices = thisRoundGraph.aggregateMessages[(String,Int,Map[Long, Integer])](
        ctx => {
          //遍历源顶点属性
          val iterator = ctx.srcAttr.filter(!_.equals(None)).iterator
          while (iterator.hasNext) {
            val (edge,performance,m) = iterator.next()
            //send msg ,同上将出度顶点传递给dst
            //ctx.sendToDst(Map(k -> newV))
            val param1 = edge+","+ctx.attr
            println(param1)
            ctx.sendToDst(param1,performance,Map(ctx.srcId -> 2))

            //ctx.sendToDst(ctx.attr,ctx.dstAttr._1,Map(ctx.srcId -> 1))
          }
        },
        (a,b) => (a._1+"->"+b._1,a._2+b._2,a._3 ++ b._3)
        /*(newAttr, oldAttr) => {
          if (oldAttr.contains(newAttr.head._1)) { // optimization to reduce msg
            oldAttr.updated(newAttr.head._1, 1) // stop sending this ever
          } else {
            oldAttr ++ newAttr
          }
        }*/
      )
      //roundVertices.collect.foreach(println(_))
      thisRoundGraph.triplets.collect.foreach(println(_))
    }
  }


  /**
    * 计算好友之间的共同好友数
    * 找邻居
    *1、每个顶点，将自己的id，发送给自己所有的邻居
    *2、每个顶点，将收到的所有邻居id，合并为一个List
    *3、对新List进行排序，并和原来的图进行关联，附到顶点之上
    * 数好友
    * 1. 遍历所有的Triplet，对2个好友的有序好友List进行扫描匹配，数出共同好友数，并将其更新到edge之上
    * */

  /**
    * 计算N度关系
    * 1、逐轮循环，找到当前顶点的邻居
    *
    * */
  def graphCalculateNDegreeRelationship(): Unit ={
    val sparkConf = new SparkConf().setAppName("graphCalculateNDegreeRelationship")
    val sc = new SparkContext(sparkConf)
    //order vertexIds (orderId,age,value)
    val vertexArray = Array(
      (1L, ("order001", 28,0)),
      (2L, ("order002", 27,1)),
      (3L, ("order003", 30,1)),
      (4L, ("order004", 18,1)),
      (5L, ("order005", 32,0)),
      (6L, ("order006", 27,0)),
      (7L, ("order007", 30,1))
    )
    //order edge
    val edgeArray = Array(
      Edge(1L, 2L, "mobile"),
      Edge(2L, 3L, "device"),
      Edge(3L, 4L, "email"),
      Edge(2L, 5L, "cid"),
      Edge(5L, 6L, "device"),
      Edge(1L, 7L, "mobile")
    )

    //数据结构，从hive读取
    /**
      * (1,2)
      * (1,7)
      * (1,2,3)
      * (1,2,5)
      * (1,2,3,4)
      * (1,2,5,6)
      * */

    //构建顶点
    val vertexRDD:RDD[(VertexId,(String,Int,Int))] = sc.parallelize(vertexArray)
    //构建边
    val edgeRDD:RDD[Edge[String]] = sc.parallelize(edgeArray)
    //rdd加载图数据
    val friendsGraph = Graph(vertexRDD,edgeRDD)

    val totalRounds: Int = 3 // total N round
    val targetVerticeID: Long = 1 // target vertice

    // 初始化所有顶点属性
    val roundGraph = friendsGraph.mapVertices((id, vd) => Map())
    var roundVertices = roundGraph.aggregateMessages[Map[Long, Integer]](
      ctx => {
        if (targetVerticeID == ctx.srcId) {
          // send msg，将出度顶点传递给dst
          ctx.sendToDst(Map(ctx.srcId -> 1))
        }
      },
      _ ++ _
    )
    //roundVertices.collect.foreach(println(_))

    for (i <- 2 to totalRounds) {
      val thisRoundGraph = roundGraph.outerJoinVertices(roundVertices){
        (vid, data, opt) => opt.getOrElse(Map[Long, Integer]())
      }
      //thisRoundGraph.triplets.collect.foreach(println(_))
      //聚合消息
      roundVertices = thisRoundGraph.aggregateMessages[Map[Long, Integer]](
        ctx => {
          //遍历源顶点属性
          val iterator = ctx.srcAttr.iterator
          while (iterator.hasNext) {
            val (k, v) = iterator.next
              //v为关联关系深度，此处递减
              val newV = v + 1
              //send msg ,同上将出度顶点传递给dst
              ctx.sendToDst(Map(k -> newV))
              //更新当前源顶点的关联深度
              ctx.srcAttr.updated(k, newV)
          }
        },
        (newAttr, oldAttr) => {
          if (oldAttr.contains(newAttr.head._1)) { // optimization to reduce msg
            oldAttr.updated(newAttr.head._1, 1) // stop sending this ever
          } else {
            oldAttr ++ newAttr
          }
        }
      )
      //roundVertices.collect.foreach(println(_))
      thisRoundGraph.triplets.collect.foreach(println(_))
    }
  }
}
*/
