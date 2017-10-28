package lakala.graphx.degreeData;

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
    //val edgeRDD = initGraph
    //runNHopResult(edgeRDD)

  }

  //get NDegree relationship
  def initGraph() ={
    //顶点对（cert_no--2,order_id）(email--3,order_id)(mobile--4,order_id)(device--5,order_id)
    val edgeArray = Array(
      Edge(hashId("421083"), hashId("YFQ001"), EdgeArr("421083","YFQ001","2","1")),
      Edge(hashId("luhuamin@lakala.com"), hashId("YFQ001"), EdgeArr("luhuamin@lakala.com","YFQ001","3","1")),
      Edge(hashId("18666956069"), hashId("YFQ001"), EdgeArr("18666956069","YFQ001","4","1")),
      Edge(hashId("18666956069"), hashId("YFQ002"), EdgeArr("18666956069","YFQ002","4","1")),
      Edge(hashId("421082"), hashId("YFQ002"), EdgeArr("421082","YFQ002","2","1")),
      Edge(hashId("devicexxx"), hashId("YFQ002"), EdgeArr("devicexxx","YFQ002","5","1")),
      Edge(hashId("devicexxx"), hashId("YFQ003"), EdgeArr("devicexxx","YFQ003","5","1")),
      Edge(hashId("lhm@lakala.com"), hashId("YFQ003"), EdgeArr("lhm@lakala.com","YFQ003","3","1")),
      Edge(hashId("lhm@lakala.com"), hashId("YFQ004"), EdgeArr("lhm@lakala.com","YFQ004","3","1")),
      Edge(hashId("134666"), hashId("YFQ004"), EdgeArr("134666","YFQ004","4","1")),
      Edge(hashId("134666"), hashId("YFQ005"), EdgeArr("134666","YFQ005","4","1")))
    val edges: RDD[Edge[EdgeArr]] = sc.parallelize(edgeArray)
    //构造图
    var graph = Graph.fromEdges(edges,"")
    graph.triplets.collect().foreach(println(_))
    println("========================================================")
    //标准图结构
    var preG: Graph[String, EdgeArr] = null
    var iterCount = 0
    //迭代次数，一度关联需迭代2次
    //while (iterCount < 2) {
      println(s"iteration $iterCount start .....")
      //原始图
      preG = graph
      //获取sendMsg之后的图--临时图
      val tmpG = graph.aggregateMessages[String](sendMsg, merageMsg).cache()
      tmpG.collect().foreach(println(_))
      //打印 tempG 所有的 msg
      //print("下一次迭代要发送的messages:" + tmpG.filter(v => StringUtils.isNotBlank(v._2)).take(10).mkString("\n"))
      //关联获取新图，更新顶点属性
      graph = graph.outerJoinVertices(tmpG) { (_, _, updateAtt) => updateAtt.getOrElse("") }
      graph.triplets.collect().foreach(println(_))
      graph.cache() //新的graph cache起来，下一次迭代使用
      //      print(g.vertices.take(10).mkString("\n"))
      tmpG.unpersist(blocking = false)
      preG.vertices.unpersist(blocking = false)
      preG.edges.unpersist(blocking = false)
      println(s"iteration $iterCount end .....")
      iterCount += 1
    //}
    //************************
    /*val result = graph.vertices
      .mapPartitions(vs => vs.filter(v => StringUtils.isNotBlank(v._2)).flatMap(v => v._2.split("-->")).filter(_.split("->").length == args(1).toInt + 1))
    result.saveAsTextFile("")*/
    graph.edges.collect().foreach(println(_))
  }

  def hashId(str: String) = {
    Hashing.md5().hashString(str, StandardCharsets.UTF_8).asLong()
  }

  //测试获取子图
  def testSubGraph(): Unit ={
    //根据边构造图
    val edgeArray = Array(
      /*Edge(hashId("order001"), hashId("luhuamin@163.com"), EdgeArr("order001","luhuamin@163.com","1","email")),
      Edge(hashId("order001"), hashId("device_20170818"), EdgeArr("order001","device_20170818","1","device")),
      Edge(hashId("order002"), hashId("device_20170818"), EdgeArr("order002","device_20170818","1","device")),
      Edge(hashId("order003"), hashId("device_20170818"), EdgeArr("order003","device_20170818","1","device")),
      Edge(hashId("order003"), hashId("mobile_20170818"), EdgeArr("order003","mobile_20170818","1","mobile")),
      Edge(hashId("order004"), hashId("luhuamin@163.com"), EdgeArr("order004","luhuamin@163.com","1","email")),
      Edge(hashId("order005"), hashId("mobile_20170818"), EdgeArr("order005","mobile_20170818","1","mobile")))*/
      Edge(1L, 11L, EdgeArr("order001","luhuamin@163.com","1","email")),
      Edge(1L, 12L, EdgeArr("order001","device_20170818","1","device")),
      Edge(2L, 12L, EdgeArr("order002","device_20170818","1","device")),
      Edge(3L, 12L, EdgeArr("order003","device_20170818","1","device")),
      Edge(3L, 13l, EdgeArr("order003","mobile_20170818","1","mobile")),
      Edge(4L, 14L, EdgeArr("order004","luhuamin@163.com","1","email")),
      Edge(5L, 13l, EdgeArr("order005","mobile_20170818","1","mobile")))


    //构造edgeRDD
    val edgeRDD: RDD[Edge[EdgeArr]] = sc.parallelize(edgeArray)
    //开始时间
    val startTime = System.currentTimeMillis()
    //构造图Graph[VD,ED]
    val graph = Graph.fromEdges(edgeRDD,"")
    //根据定点查询子图
     graph.subgraph(epred = edgeRDD => edgeRDD.attr.srcValue == "order001").edges.collect().foreach(e => println(e.attr.srcValue+"-->"+e.attr.dstValue))
    //耗时
    println("总共耗时："+(System.currentTimeMillis()-startTime)/1000.0+" seconds")
    //graph.subgraph(vpred = (vid, v) => v._2 >= 200).vertices.collect.foreach(println(_))
    //graph.subgraph(epred = edge => edge.attr._1 >= 200).edges.collect.foreach(println(_))
    //val subGraph = graph.subgraph(vpred = (vid, v) => v._2 >= 200, epred = edge => edge.attr._1 >= 200)


  }

  //查询N度关联子图
  def runNHopResult(edges:RDD[Edge[EdgeArr]]): Unit = {
    //根据edge构造图
    var graph = Graph.fromEdges(edges,"")
    println("========================")
    var preG: Graph[String, EdgeArr] = null
    var iterCount = 0
    //迭代次数，一度关联需迭代2次
    while (iterCount < 2) {
      println(s"iteration $iterCount start .....")
      //原始图--临时图
      preG = graph
      //获取sendMsg之后的图--临时图
      val tmpG = graph.aggregateMessages[String](sendMsg, merageMsg).cache()
      //
      print("下一次迭代要发送的messages:" + tmpG.filter(v => StringUtils.isNotBlank(v._2)).take(10).mkString("\n"))
      graph = graph.outerJoinVertices(tmpG) { (_, _, updateAtt) => updateAtt.getOrElse("") }

      graph.cache() //新的graph cache起来，下一次迭代使用
      //      print(g.vertices.take(10).mkString("\n"))
      tmpG.unpersist(blocking = false)
      preG.vertices.unpersist(blocking = false)
      preG.edges.unpersist(blocking = false)
      println(s"iteration $iterCount end .....")
      iterCount += 1
    }
    //************************
    /*val result = graph.vertices
      .mapPartitions(vs => vs.filter(v => StringUtils.isNotBlank(v._2)).flatMap(v => v._2.split("-->")).filter(_.split("->").length == args(1).toInt + 1))
    result.saveAsTextFile("")*/
  }

  def judgSendMsg = (sendType: Array[String], edge: EdgeArr) => {
    var flag = false
    for (stype <- sendType) if (edge.srcType.equals(stype)) flag = true
    flag
  }

  //定义发送消息
  def sendMsg(ctx: EdgeContext[String, EdgeArr, String]): Unit = {
    //初始状态，关联属性为空 && 顶点属性为空
    if ((ctx.srcAttr == null || ctx.srcAttr.length == 0) && (ctx.dstAttr == null || ctx.dstAttr.length == 0)) {
      //由于图构建方式，sendToSrc回溯
      //msg: orderId->cid&2、orderId->email&3、orderId->mobile&4
      ctx.sendToSrc(s"${ctx.attr.dstValue}->${ctx.attr.srcValue}&${ctx.attr.srcType}")
    }

    //第2轮迭代，关联属性不为空 && orderId属性为空
    if (!(ctx.srcAttr == null || ctx.srcAttr.length == 0)  && (ctx.dstAttr == null || ctx.dstAttr.length == 0)) {
      //判断关联属性 存在-->,表示存在进件之间关联
      ctx.srcAttr.split("-->").map { att =>
        //??
        if (!att.contains(s"${ctx.attr.dstValue}")) {
          ctx.sendToDst(s"$att->${ctx.attr.dstValue}")
        }
        ctx.sendToSrc("")
      }
    }

    /*if (StringUtils.isEmpty(ctx.srcAttr) && StringUtils.isNotEmpty(ctx.dstAttr)) {
      ctx.dstAttr.split("-->").map { att =>
        //          if (att.endsWith(ctx.attr.dstV) && !att.contains(s"${ctx.attr.srcV}&${ctx.attr.srcType}->${ctx.attr.dstV}")) {
        if (!att.contains(s"${ctx.attr.srcValue}&${ctx.attr.srcType}")) {
          ctx.sendToSrc(s"$att->${ctx.attr.srcValue}&${ctx.attr.srcType}")
        }
        ctx.sendToDst("")
      }
    }*/
  }

  //合并消息 msg: orderId->cid&2、orderId->email&3、orderId->mobile&4
  def merageMsg = (msgA: String, msgB: String) => {
    //多条入边顶点
    if (msgA.equals(msgB)) {
      msgA
    } else if (msgA.contains(msgB)) msgA
    else if (msgB.contains(msgA)) msgB
    else {
      //合并入边
      msgA + "-->" + msgB
    }
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

  //构建Edge，从Hive读取数据
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
