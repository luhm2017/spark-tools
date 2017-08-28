package lakala.graphx

import java.nio.charset.StandardCharsets

import com.google.common.hash.Hashing
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/4/9.
  */
object GraphXExample {


  def main(args: Array[String]): Unit = {
    //
    val sparkConf = new SparkConf().setAppName("GraphXExample")
    val sc = new SparkContext(sparkConf)

    //顶点VD:(String,Int)、边
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50)))
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3))

    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    //构造图Graph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    println("***********************************************")
    println("属性演示")
    println("**********************************************************")
    println("找出图中年龄大于30的顶点：")
    graph.vertices.filter { case (id, (name, age)) => age > 30}.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }

    //边操作：找出图中属性大于5的边
    println("找出图中属性大于5的边：")
    graph.edges.filter(e => e.attr > 5).collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println

    //triplets操作，((srcId, srcAttr), (dstId, dstAttr), attr)
    println("列出边属性>5的tripltes：")
    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }
    println

    //Degrees操作
    println("找出图中最大的出度、入度、度数：")
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    println("max of outDegrees:" + graph.outDegrees.reduce(max))
    println(" max of inDegrees:" + graph.inDegrees.reduce(max))
    println( "max of Degrees:" + graph.degrees.reduce(max))
    println

    println("**********************************************************")
    println("转换操作")
    println("**********************************************************")
    println("顶点的转换操作，顶点age + 10：")
    graph.mapVertices{ case (id, (name, age)) => (id, (name, age+10))}
      .vertices.collect.foreach(
        v => println(s"${v._2._1} is ${v._2._2}")
    )
    println

    println("边的转换操作，边的属性*2：")
    graph.mapEdges(e=>e.attr*2).edges
      .collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println

    //**********************************************************************************
    println("**********************************************************")
    println("结构操作")
    println("**********************************************************")
    println("顶点年纪>30的子图：")
    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 30)
    println("子图所有顶点：")
    subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    println
    println("子图所有边：")
    subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println

    //***************************  连接操作    ****************************************
    //**********************************************************************************
    println("**********************************************************")
    println("连接操作")
    println("**********************************************************")
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    //每个顶点的入度数VertexRDD[graphx.VertexId,Int]
    inDegrees.collect().foreach(println(_))
    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
    //创建一个新图，顶点VD的数据类型为User，并从graph做类型转换
    val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0)}
    //initialUserGraph与inDegrees、outDegrees（RDD）进行连接，并修改initialUserGraph中inDeg值、outDeg值
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg,outDegOpt.getOrElse(0))
    }

    println("连接图的属性：")
    userGraph.vertices.collect.foreach(v => println(s"${v._2.name} inDeg: ${v._2.inDeg} outDeg: ${v._2.outDeg}"))
    println
    println("出度和入读相同的人员：")
    userGraph.vertices.filter {
      case (id, u) => u.inDeg == u.outDeg
    }.collect.foreach {
      case (id, property) => println(property.name)
    }
    println

    //***********************************************************************************
    //***************************  聚合操作    ****************************************
    //**********************************************************************************
    userGraph.edges.collect().foreach(println(_))
    graph.edges.collect().foreach(println(_))
    userGraph.vertices.collect().foreach(println(_))
    graph.vertices.collect().foreach(println(_))


    println("**********************************************************")
    println("聚合操作")
    println("**********************************************************")
    println("找出年纪最大的追求者：")
    //顶点属性(name,age)
    //aggregateMessages 返回VertexRDD，包含每个去往顶点的聚合消息
    val oldestFollower: VertexRDD[(String, Int)] = userGraph.aggregateMessages[(String, Int)](
      // 将源顶点的属性发送给目标顶点，map过程
      //以边为维度
      edge => Iterator((edge.dstId, (edge.srcAttr.name, edge.srcAttr.age))),
      // 得到最大追求者，reduce过程
      (a, b) => if (a._2 > b._2) a else b
    )
    //oldestFollower.collect().foreach(println(_))
    userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
      optOldestFollower match {
        case None => s"${user.name} does not have any followers."
        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
      }
    }.collect.foreach { case (id, str) => println(str)}
    println

    //新操作
    //==================================================================================
    val firstFollower = graph.aggregateMessages[(String, Int)] (sendMsg,mergeMsg)
    firstFollower.collect().foreach(println(_))
    //***********************************************************************************
    //***************************  实用操作    ****************************************
    //**********************************************************************************
    println("**********************************************************")
    println("聚合操作")
    println("**********************************************************")
    println("找出5到各顶点的最短：")
    val sourceId: VertexId = 5L // 定义源点
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist),
      triplet => {  // 计算权重
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a,b) => math.min(a,b) // 最短距离
    )
    println(sssp.vertices.collect.mkString("\n"))
  }

  //定义发送消息，(vertex、edge、msg)讲源顶点的年龄发送给目标顶点
  def sendMsg(ctx: EdgeContext[(String,Int), Int, (String,Int)]): Unit = {
    println("-----------------vertexId:"+ctx.srcId+"name:"+ctx.srcAttr._1+",age:"+ctx.srcAttr._2)
    //讲src顶点属性 发送给dst顶点
    ctx.sendToDst(ctx.srcAttr)
  }

  //合并消息Msg
  def mergeMsg = (msgA: (String,Int),msgB: (String,Int)) => {
    (msgA._1+""+msgB._1,msgA._2 + msgB._2)
  }

  //apachecn spark graphx example
  def graphxDemo(): Unit ={
    val conf = new SparkConf().setAppName("graphxDemo")
    val sc = new SparkContext(conf)
    //顶点VD:(String,Int)、边
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50)))
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3))

    // Create an RDD for the vertices
    val vertexs: RDD[(VertexId, (String, Int))] = sc.parallelize(vertexArray)
    val edges: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    // Build the initial Graph
    val graph = Graph(vertexs, edges)

    //========================================================================
    // Count all users which are postdocs
    graph.vertices.filter { case (id, (name, age)) => age > 30 }.count
    // Count all the edges where src > dst
    graph.edges.filter(e => e.srcId > e.dstId).count

    // Compute the number of older followers and their total age
    val olderFollowers: VertexRDD[(Int, Int)] = graph.aggregateMessages[(Int, Int)](
      triplet => { // Map Function
        if (triplet.srcAttr._2 > triplet.dstAttr._2) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst(1, triplet.srcAttr._2)
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    olderFollowers.collect().foreach(println(_))
    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
    olderFollowers.mapValues( (id, value) =>
      value match { case (count, totalAge) => totalAge / count } )
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println(_))
  }

  //获取确定源点的子图
  def subgraphDemo(): Unit ={
    val conf = new SparkConf().setAppName("graphxDemo")
    val sc = new SparkContext(conf)
    //顶点VD:(String,Int)、边
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50)))
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3))

    // Create an RDD for the vertices
    val vertexs: RDD[(VertexId, (String, Int))] = sc.parallelize(vertexArray)
    val edges: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    // Build the initial Graph
    val graph = Graph(vertexs, edges)
    /*graph.triplets.collect().foreach(println(_))
    graph.vertices.collect().foreach(println(_))
    graph.edges.collect().foreach(println(_))*/

    //源顶点
    //val choosedVertex: RDD[VertexId] = sc.parallelize(Array((5L, ("Ed", 55))))
    // aggregateMessages 运算符返回一个 VertexRDD[Msg] ，其中包含去往每个顶点的聚合消息（Msg类型）
    val subGraph: VertexRDD[String] = graph.aggregateMessages[String](subSendMsg,subMergeMsg)
    subGraph.collect().foreach(println(_))

  }

  //send msg
  //它将源和目标属性以及 edge 属性和函数 (sendToSrc, 和 sendToDst) 一起发送到源和目标属性
  def subSendMsg(ctx:EdgeContext[(String, Int),Int,String]): Unit ={
      //
      ctx.sendToDst(ctx.srcAttr._1+"||"+ctx.attr+"-->")
  }

  //mergMsg
  //用户定义的 mergeMsg 函数需要两个发往同一顶点的消息，并产生一条消息
  def subMergeMsg = (msgA: String,msgB: String) => {
      msgA+msgB
  }

  def hashId(str: String) = {
    Hashing.md5().hashString(str, StandardCharsets.UTF_8).asLong()
  }

  //定义边属性
  case class EdgeArr(srcV: String, dstV: String, srcType: String, dstType: String)

  /**
    * 获取关联子图
    * 1、剪枝 subgraph  判断依据 degree 0,1，大于阀值100000等
    * 2、根据顶点类型（关联属性），获取关联顶点对
    * */
  def graphDemo1(): Unit ={
    val conf = new SparkConf().setAppName("graphxDemo")
    val sc = new SparkContext(conf)
    //type =1 表示orderId,0--关联属性实体,初始出度、入度都为0
    val vertexArray = Array(
      (hashId("YFQ001"), ("YFQ001",1)),
      (hashId("YFQ002"), ("YFQ002",1)),
      (hashId("YFQ003"), ("YFQ003",1)),
      (hashId("YFQ004"), ("YFQ004",1)),
      (hashId("YFQ005"), ("YFQ005",1)),
      (hashId("421083"), ("421083",0)),
      (hashId("luhuamin@lakala.com"), ("luhuamin@lakala.com",0)),
      (hashId("18666956069"), ("18666956069",0)),
      (hashId("421082"), ("421082",0)),
      (hashId("lhm@lakala.com"), ("lhm@lakala.com",0)),
      (hashId("134666"), ("134666",0)),
      (hashId("YFQ006"), ("YFQ006",1))
    )
    //顶点对(order_id--1)（cert_no--2）(email--3)(mobile--4)(device--5)
    val edgeArray = Array(
      Edge(hashId("421083"), hashId("YFQ001"), EdgeArr("421083","YFQ001","2","1")),
      Edge(hashId("luhuamin@lakala.com"), hashId("YFQ001"), EdgeArr("luhuamin@lakala.com","YFQ001","3","1")),
      Edge(hashId("18666956069"), hashId("YFQ001"), EdgeArr("18666956069","YFQ001","4","1")),
      Edge(hashId("18666956069"), hashId("YFQ002"), EdgeArr("18666956069","YFQ002","4","1")),
      Edge(hashId("421082"), hashId("YFQ002"), EdgeArr("421082","YFQ002","2","1")),
      //Edge(hashId("devicexxx"), hashId("YFQ002"), EdgeArr("devicexxx","YFQ002","5","1")),
      //Edge(hashId("devicexxx"), hashId("YFQ003"), EdgeArr("devicexxx","YFQ003","5","1")),
      Edge(hashId("lhm@lakala.com"), hashId("YFQ003"), EdgeArr("lhm@lakala.com","YFQ003","3","1")),
      Edge(hashId("lhm@lakala.com"), hashId("YFQ004"), EdgeArr("lhm@lakala.com","YFQ004","3","1")),
      Edge(hashId("134666"), hashId("YFQ004"), EdgeArr("134666","YFQ004","4","1")),
      Edge(hashId("134666"), hashId("YFQ005"), EdgeArr("134666","YFQ005","4","1")),
      Edge(hashId("lhm@lakala.com"), hashId("YFQ006"), EdgeArr("lhm@lakala.com","YFQ006","3","1")))
    val edges: RDD[Edge[EdgeArr]] = sc.parallelize(edgeArray)
    val vertexs:RDD[(VertexId, (String,Int))] = sc.parallelize(vertexArray)
    // Build the initial Graph
    val graph =  Graph(vertexs,edges)
    //计算每个顶点的入度、出度，然后赋值给顶点属性
    //每个顶点添加入度、出度属性
    val initGraph =  graph.mapVertices { case (id, (vertexValue, vertexType)) => (vertexValue, vertexType, 0, 0)}
    //initGraph与inDegrees、outDegrees（RDD）进行连接，并更新initGraph中inDeg值、outDeg值
    val newGraph = initGraph.outerJoinVertices(initGraph.inDegrees) {
      case (id, u, inDegOpt) => (u._1, u._2, inDegOpt.getOrElse(0), u._4)
    }.outerJoinVertices(initGraph.outDegrees) {
      case (id, u, outDegOpt) => (u._1, u._2, u._3,outDegOpt.getOrElse(0))
    }

    //剪枝，保留条件：顶点type==1 或者 type==0&&outDegree >=2
    val perGraph = newGraph.subgraph(
        vpred =(id,attr) =>
          //vertexType =1 表示进件顶点 、 vertexType = 0表示关联顶点 && vertex
          attr._2 == 1 || (attr._2 == 0 && attr._4 >= 2)
    )

    //1、出度为>=2并且为关联属性的顶点，计算所有的一度关联关系
    //返回一个 VertexRDD[Msg],msg格式与sendMsg[A]一致，(String,String)
    val tempG = perGraph.aggregateMessages[String](sendMsgNew,mergeMsgNew)
    //保存一度关联数据


    //2、然后入度>=2并且为进件属性的顶点，计算所有的二度关系
    //val tmpG = graph.aggregateMessages[String](sendMsg, merageMsg).cache()

    //print graph
    graph.triplets.collect().foreach(println(_))
    newGraph.triplets.collect().foreach(println(_))
    perGraph.triplets.collect().foreach(println(_))
    perGraph.vertices.collect().foreach(println(_))
    tempG.collect().foreach(println(_))

  }

  //发送消息，edge默认参数[VD, ED, A]，A--自定义msg格式
  def sendMsgNew(ctx: EdgeContext[(String,Int,Int,Int), EdgeArr, (String)]): Unit ={
    //户定义的 sendMsg 函数接受一个边缘三元组 EdgeContext
    //它将源和目标属性以及 edge 属性和函数 (sendToSrc, 和 sendToDst) 一起发送到源和目标属性
    if(ctx.srcAttr._4 >=2)
    //关联实体dst 发送到 进件顶点src-->(YFQ003,email)
      ctx.sendToSrc(ctx.attr.dstV+"&"+ctx.attr.srcType)
  }

  //合并消息，mergeMsg 函数需要两个发往同一顶点的消息，并产生一条消息
  def mergeMsgNew = (msgA: String,msgB: String) =>{
    //合并消息
    msgA+"||"+msgB
  }

  //保存一度关联的数据
  def saveOneDegree(tempG: RDD[(VertexId,String)]): Unit ={
      //
      val oneDegreeData = tempG.mapPartitions(
          lines => lines.map{
             row =>
               //val oneDegreeList = ListBuffer[(String,String,String,String)]()
               val list = row._2//.split("||")
               "YFQ001&4||YFQ002&4".split("||").length
               //YFQ002&4||YFQ001&4
               //YFQ003&3||YFQ004&3||YFQ006&3
               /*for(i <- 0 until list.length){
                    for(j <- i+1 until list.length){
                      val tempA = list(i)
                      val tempB = list(j)
                      println(tempA.split("&")(0)+""+tempA.split("&")(1)+""+tempB.split("&")(1)+""+tempB.split("&")(0))
                      (tempA.split("&")(0),tempA.split("&")(1),tempB.split("&")(1),tempB.split("&")(0))
                    }
               }*/
               list
               //(list(0),list(1),list(2))
          }
      )
      oneDegreeData.collect().foreach(println(_))


  }

}
