package lakala.graphx.graphxData

import lakala.graphx.util.DateTimeUtils
import lakala.graphx.util.UtilsToos.{hashId, isMobileOrPhone}
import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.{SaveMode, SparkSession}

import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by linyanshi on 2017/11/13 0013.
  * updated by luhuamin， 兼容spark1.6和spark2.2的运行环境
  */
object ExportGraphDataDay extends Serializable {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    println(s"start time ${DateTimeUtils.formatter.print(start)}")
    val gbsp = new ExportGraphDataDay(args)
    gbsp.runDegreeResult()
    val end = System.currentTimeMillis()
    println(s"end time ${DateTimeUtils.formatter.print(end)} run working time ${(end - start) / 60000} minute")
    // F:\\lakalaFinance_workspaces\\graphx-analysis\\apply\bin\\test.csv 4 F:\\out\\output "2012-12-01 00:00:00" "2017-10-10 23:59:59"
  }
}


/*extends Serializable*/

class ExportGraphDataDay(args: Array[String]) extends Serializable {
  val maxOutDegree = 10000
  //@transient
  val conf = new SparkConf().setAppName("ExportGraphData")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.task.maxFailures", "8")
  conf.set("spark.yarn.max.executor.failures", "100")
  conf.set("spark.shuffle.consolidateFiles", "true")
  conf.set("spark.shuffle.compress", "true")
  conf.set("spark.rdd.compress", "true")
  conf.set("spark.kryoserializer.buffe", "512m")
  conf.set("hive.merge.sparkfiles", "true")
  conf.set("hive.exec.max.dynamic.partitions", "10000")
  conf.registerKryoClasses(Array(classOf[NewEdgeArrDT]))
  GraphXUtils.registerKryoClasses(conf)
  //add by luhuamin
  val sc = new SparkContext(conf)
  //val hc = new HiveContext(sc)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  //@transient
  /*val spark = SparkSession.builder().config(conf)
    .config("spark.sql.warehouse.dir", "hdfs:///user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    */
  sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

  def runDegreeResult(): Unit = {
    var g: Graph[ArrayBuffer[String], NewEdgeArrDT] = Graph.fromEdges(loadApplyHive, ArrayBuffer[String](), edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER)
    g = g.outerJoinVertices(g.outDegrees)((_, old, deg) => (old, deg.getOrElse(0)))
      .subgraph(vpred = (_, a) => a._2 < maxOutDegree).mapVertices((_, vdeg) => vdeg._1)

    var preG: Graph[ArrayBuffer[String], NewEdgeArrDT] = null
    var iterCount: Int = 0
    while (iterCount < args(1).toInt) {
      println(s"iteration $iterCount start .....")
      preG = g
      var tmpG = g.aggregateMessages[ArrayBuffer[String]](sendMsg, merageMsg).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
      g = g.outerJoinVertices(tmpG) { (_, _, updateAtt) => updateAtt.getOrElse(ArrayBuffer[String]()) }
      g.persist(StorageLevel.MEMORY_AND_DISK_SER)
      iterCount match {
        case 1 => saveOneDegreeData(g, 2)
        case 3 => saveTwoDegreeData(g, 4)
        case _ =>
      }
      tmpG.unpersist(blocking = false)
      preG.vertices.unpersist(blocking = false)
      preG.edges.unpersist(blocking = false)
      println(s"iteration $iterCount end .....")
      iterCount += 1
    }
    //************************

  }

  def saveOneDegreeData(g: Graph[ArrayBuffer[String], NewEdgeArrDT], degree: Int): Unit = {
    println(s"${DateTimeUtils.formatter.print(System.currentTimeMillis())} start load ${degree - 1} to hive")
    val result = g.vertices
      .mapPartitions(vs => vs.filter(v => v._2.nonEmpty)
        .flatMap(v => v._2.toArray.filter(s => s.count(c => c.equals(',')) == degree)
          .map { l =>
            val line = l.replaceAll("&", ",").replaceAll("#", ",")
            val arr = s"${line},${line.substring(0, 10)}".split(",")
            (arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6))
          }))

    val (cols, parquetTable) = colsAndParquetTable(degree)
    /*import spark.implicits._
    spark.sql("use lkl_card_score_dev")*/
    import sqlContext.implicits._
    sqlContext.sql("use lkl_card_score_dev")
    result.toDF(cols: _*).write.mode(SaveMode.Append).format("hive")
      .partitionBy(cols(cols.length - 1)).saveAsTable(parquetTable)
    println(s"${DateTimeUtils.formatter.print(System.currentTimeMillis())} end load ${degree - 1} to hive")
  }

  def saveTwoDegreeData(g: Graph[ArrayBuffer[String], NewEdgeArrDT], degree: Int): Unit = {
    println(s"${DateTimeUtils.formatter.print(System.currentTimeMillis())} start load ${degree - 2} to hive")
    val result = g.vertices
      .mapPartitions(vs => vs.filter(v => v._2.nonEmpty)
        .flatMap(v => v._2.toArray.filter(s => s.count(c => c.equals(',')) == degree)
          .map { l =>
            val line = l.replaceAll("&", ",").replaceAll("#", ",")
            val arr = s"${line},${line.substring(0, 10)}".split(",")
            (arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7), arr(8), arr(9), arr(10))
          }))

    val (cols, parquetTable) = colsAndParquetTable(degree)
    /*import spark.implicits._
    spark.sql("use lkl_card_score_dev")*/
    import sqlContext.implicits._
    sqlContext.sql("use lkl_card_score_dev")
    result.toDF(cols: _*).write.mode(SaveMode.Append).format("hive")
      .partitionBy(cols(cols.length - 1)).saveAsTable(parquetTable)
    println(s"${DateTimeUtils.formatter.print(System.currentTimeMillis())} end load ${degree - 2} to hive")
  }

  def colsAndParquetTable(degree: Int) = {
    degree match {
      case 2 => (Array("a_time", "a_orderno", "edge_value1", "edge_name1", "b_time", "b_orderno", "dt"), "one_degree_apply_result_orcfile")
      case 4 => (Array("a_time", "a_orderno", "edge_value1", "edge_name1", "b_time", "b_orderno", "edge_value2", "edge_name2", "c_time", "c_orderno", "dt"), "two_degree_apply_result_orcfile")
    }
  }

  def sendMsg(ctx: EdgeContext[ArrayBuffer[String], NewEdgeArrDT, ArrayBuffer[String]]): Unit = {
    if (ctx.srcAttr.isEmpty && ctx.dstAttr.isEmpty && ctx.attr.init) {
      ctx.sendToSrc(ArrayBuffer[String](s"${ctx.attr.timeStr}#${ctx.attr.dstV},${ctx.attr.srcV}&${ctx.attr.srcType}"))
    }

    if (ctx.srcAttr.nonEmpty && ctx.dstAttr.isEmpty) {
      val filterArray = ctx.srcAttr.filter(v => !v.contains(s"${ctx.attr.timeStr}#${ctx.attr.dstV}"))
        .filter { v =>
          val src_time = v.substring(0, v.indexOf("#"))
          filterGtTime(src_time, ctx.attr.timeStr)
        }
      val toDstArrayBuffer = new ArrayBuffer[String]()

      for (i <- 0 until (filterArray.size)) {
        toDstArrayBuffer.insert(i, s"${filterArray(i)},${ctx.attr.timeStr}#${ctx.attr.dstV}")
      }
      ctx.sendToDst(toDstArrayBuffer)
      ctx.sendToSrc(ArrayBuffer[String]())
    }

    if (ctx.srcAttr.isEmpty && ctx.dstAttr.nonEmpty) {
      val filterArray = ctx.dstAttr
        .filter(v => !v.contains(s"${ctx.attr.timeStr}#${ctx.attr.dstV},${ctx.attr.srcV}&${ctx.attr.srcType}"))
      val toSrcArrayBuffer = new ArrayBuffer[String]()
      for (i <- 0 until (filterArray.size)) {
        toSrcArrayBuffer.insert(i, s"${filterArray(i)},${ctx.attr.srcV}&${ctx.attr.srcType}")
      }
      ctx.sendToSrc(toSrcArrayBuffer)
      ctx.sendToDst(ArrayBuffer[String]())
    }

  }


  def merageMsg = (msgAarr: ArrayBuffer[String], msgBarr: ArrayBuffer[String]) => {
    msgAarr.union(msgBarr).distinct
  }

  def filterGtTime(src_time: String, appyly_time: String): Boolean = {
    var init = false
    try {
      init = if (DateTimeUtils.formatter.parseMillis(src_time) > DateTimeUtils.formatter.parseMillis(appyly_time)) true
      else false
    } catch {
      case e: Exception =>
    }
    init
  }

  //orderId,contractNo,termId,loanPan,returnPan,insertTime,recommend,userId,
  // deviceId
  //certNo,email,company,mobile,compAddr,compPhone,emergencyContactMobile,contactMobile,ipv4,msgphone,telecode
  def loadApplyHive(): RDD[Edge[NewEdgeArrDT]] = {
    val date = args(2).substring(0, 10).split("-")
    val year = date(0)
    val month = date(1)
    val day = date(2)
    println(s"year:${year} month:${month} day${day}")
    /*spark.sql("use creditloan")*/
    sqlContext.sql("use lkl_card_score_dev")
    val sql =
      s"""select aa.order_id,aa.contract_no,aa.term_id,aa.loan_pan,aa.return_pan,aa.insert_time,aa.recommend,aa.user_id,bb.cert_no,bb.email,bb.company,bb.mobile,bb.comp_addr,bb.comp_phone,bb.emergency_contact_mobile,bb.contact_mobile,bb.ipv4,bb.msgphone,bb.telecode,cc.device_id
         |from (select a.order_id,a.contract_no,a.term_id,a.loan_pan,a.return_pan,a.insert_time,a.recommend,a.id as user_id
         |         from creditloan.s_c_loan_apply a
         |            where a.year="${year}" and a.month="${month}" and a.day="${day}" ) aa
         |left join(select c.order_no,c.device_id
         |              from creditloan.s_c_loan_deviceidauth c
         |                  where c.year="${year}" and c.month="${month}" and c.day="${day}") cc
         | on aa.order_id =cc.order_no
         	|left join (select b.cert_no,b.email,b.company,b.mobile,b.comp_addr,b.comp_phone,b.emergency_contact_mobile,b.contact_mobile,b.ipv4,b.msgphone,b.telecode,b.id as user_id
         |               from creditloan.s_c_apply_user b
         |                  where b.year="${year}" and b.month="${month}" and b.day="${day}") bb
         | on aa.user_id =bb.user_id   """.stripMargin
    println(s"sql@@@@@:${sql}")
    //val df = spark.sql(sql)
    val df = sqlContext.sql(sql)
    val lineRDD = df.rdd.mapPartitions { rows =>
      rows.flatMap { row =>
        val orderno = row.getAs[String]("order_id")
        val contractNo = if (StringUtils.isNotBlank(row.getAs[String]("contract_no")) && !"null".equals(row.getAs[String]("contract_no").toLowerCase)) row.getAs[String]("contract_no").replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""
        val termId = if (StringUtils.isNotBlank(row.getAs[String]("term_id")) && !"null".equals(row.getAs[String]("term_id").toLowerCase)) row.getAs[String]("term_id").replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""
        val loan_pan = if (StringUtils.isNotBlank("" + row.getAs[Int]("loan_pan")) && !"null".equals((("" + row.getAs[Int]("loan_pan")).toLowerCase))) ("" + row.getAs[Int]("loan_pan")).replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""
        val return_pan = if (StringUtils.isNotBlank(row.getAs[String]("return_pan")) && !"null".equals(row.getAs[String]("return_pan").toLowerCase)) row.getAs[String]("return_pan").replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""
        val insertTime = if (StringUtils.isNotBlank(row.getAs[String]("insert_time")) && !"null".equals(row.getAs[String]("insert_time").toLowerCase)) row.getAs[String]("insert_time").replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""
        val recommend = if (StringUtils.isNotBlank(row.getAs[String]("recommend")) && !"null".equals(row.getAs[String]("recommend").toLowerCase) && isMobileOrPhone(row.getAs[String]("recommend"))) row.getAs[String]("recommend").replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""
        val userId = if (StringUtils.isNotBlank("" + row.getAs[Int]("user_id")) && !"null".equals(("" + row.getAs[Int]("user_id")).toLowerCase)) ("" + row.getAs[Int]("user_id")).replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""
        val certNo = if (StringUtils.isNotBlank(row.getAs[String]("cert_no")) && !"null".equals(row.getAs[String]("cert_no").toLowerCase)) row.getAs[String]("cert_no").replaceAll(",", "") else ""
        val email = if (StringUtils.isNotBlank(row.getAs[String]("email")) && !"null".equals(row.getAs[String]("email").toLowerCase)) row.getAs[String]("email").replaceAll(",", "").replaceAll("&", "@").replaceAll("&", "@").replaceAll("#", "") else ""
        val company = if (StringUtils.isNotBlank(row.getAs[String]("company")) && !"null".equals(row.getAs[String]("company").toLowerCase)) row.getAs[String]("company").replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""
        val mobile = if (StringUtils.isNotBlank(row.getAs[String]("mobile")) && !"null".equals(row.getAs[String]("mobile").toLowerCase) && isMobileOrPhone(row.getAs[String]("mobile"))) row.getAs[String]("mobile").replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""
        val compAddr = if (StringUtils.isNotBlank(row.getAs[String]("comp_addr")) && !"null".equals(row.getAs[String]("comp_addr").toLowerCase)) row.getAs[String]("comp_addr").replaceAll(",", "_").replaceAll("&", "@").replaceAll("#", "") else ""
        val compPhone = if (StringUtils.isNotBlank(row.getAs[String]("comp_phone")) && !"null".equals(row.getAs[String]("comp_phone").toLowerCase) && isMobileOrPhone(row.getAs[String]("comp_phone"))) row.getAs[String]("comp_phone").replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""
        val emergencymobile = if (StringUtils.isNotBlank(row.getAs[String]("emergency_contact_mobile")) && !"null".equals(row.getAs[String]("emergency_contact_mobile").toLowerCase) && isMobileOrPhone(row.getAs[String]("emergency_contact_mobile"))) row.getAs[String]("emergency_contact_mobile").replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""
        val contact_mobile = if (StringUtils.isNotBlank(row.getAs[String]("contact_mobile")) && !"null".equals(row.getAs[String]("contact_mobile").toLowerCase) && isMobileOrPhone(row.getAs[String]("contact_mobile"))) row.getAs[String]("contact_mobile").replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""
        val ipv4 = if (StringUtils.isNotBlank(row.getAs[String]("ipv4")) && !"null".equals(row.getAs[String]("ipv4").toLowerCase)) row.getAs[String]("ipv4").replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""
        val msgphone = if (StringUtils.isNotBlank(row.getAs[String]("msgphone")) && !"null".equals(row.getAs[String]("msgphone").toLowerCase) && isMobileOrPhone(row.getAs[String]("msgphone"))) row.getAs[String]("msgphone").replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""
        val hometel = if (StringUtils.isNotBlank(row.getAs[String]("telecode")) && !"null".equals(row.getAs[String]("telecode").toLowerCase) && isMobileOrPhone(row.getAs[String]("telecode"))) row.getAs[String]("telecode").replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""
        val device_id = if (StringUtils.isNotBlank(row.getAs[String]("device_id")) && !"null".equals(row.getAs[String]("device_id").toLowerCase)) row.getAs[String]("device_id").replaceAll(",", "").replaceAll("&", "@").replaceAll("#", "") else ""

        var dt = if (insertTime.indexOf(".") > 0) insertTime.substring(0, insertTime.indexOf(".")) else insertTime
        val init = jugeInit(dt)
        val edgeList = ListBuffer[Edge[NewEdgeArrDT]]()
        if (StringUtils.isNotBlank(loan_pan)) {
          edgeList.+=(Edge(hashId(s"${loan_pan}&4"), hashId(orderno),
            NewEdgeArrDT(loan_pan, orderno, dt, "4", init)))
        }

        if (StringUtils.isNotBlank(return_pan)) {
          edgeList.+=(Edge(hashId(s"${return_pan}&5"), hashId(orderno),
            NewEdgeArrDT(return_pan, orderno, dt, "5", init)))
        }

        if (StringUtils.isNotBlank(recommend)) {
          edgeList.+=(Edge(hashId(s"${recommend}&7"), hashId(orderno),
            NewEdgeArrDT(recommend, orderno, dt, "7", init)))
        }

        if (StringUtils.isNotBlank(device_id) &&
          !"00000000-0000-0000-0000-000000000000".equals(device_id)) {
          edgeList.+=(Edge(hashId(s"${device_id}&9"), hashId(orderno),
            NewEdgeArrDT(device_id, orderno, dt, "9", init)))
        }
        if (StringUtils.isNotBlank(certNo)) {
          edgeList.+=(Edge(hashId(s"${certNo}&10"), hashId(orderno),
            NewEdgeArrDT(certNo, orderno, dt, "10", init)))
        }

        if (StringUtils.isNotBlank(email)) {
          edgeList.+=(Edge(hashId(s"${email}&11"), hashId(orderno),
            NewEdgeArrDT(email, orderno, dt, "11", init)))
        }

        if (StringUtils.isNotBlank(mobile)) {
          edgeList.+=(Edge(hashId(s"${mobile}&13"), hashId(orderno),
            NewEdgeArrDT(mobile, orderno, dt, "13", init)))
        }

        if (StringUtils.isNotBlank(emergencymobile)) {
          edgeList.+=(Edge(hashId(s"${emergencymobile}&16"), hashId(orderno),
            NewEdgeArrDT(emergencymobile, orderno, dt, "16", init)))
        }
        if (StringUtils.isNotBlank(contact_mobile)) {
          edgeList.+=(Edge(hashId(s"${contact_mobile}&17"), hashId(orderno),
            NewEdgeArrDT(contact_mobile, orderno, dt, "17", init)))
        }


        if (StringUtils.isNotBlank(msgphone)) {
          edgeList.+=(Edge(hashId(s"${msgphone}&19"), hashId(orderno),
            NewEdgeArrDT(msgphone, orderno, dt, "19", init)))
        }

        if (StringUtils.isNotBlank(hometel)) {
          edgeList.+=(Edge(hashId(s"${hometel}&20"), hashId(orderno),
            NewEdgeArrDT(hometel, orderno, dt, "20", init)))
        }
        edgeList
      }
    }
    lineRDD
  }

  def jugeInit(dataDt: String): Boolean = {
    var init = false
    try {
      init = if (DateTimeUtils.formatter.parseMillis(dataDt) >= DateTimeUtils.parseDataString(args(2)).minusDays(4).getMillis
        && DateTimeUtils.formatter.parseMillis(dataDt) <= DateTimeUtils.parseDataString(args(3)).minusDays(4).getMillis) true
      else false
    } catch {
      case e: Exception =>
    }
    init
  }

}
