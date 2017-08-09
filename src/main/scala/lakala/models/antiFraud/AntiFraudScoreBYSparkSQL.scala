package lakala.models.antiFraud

import java.util.Properties

import lakala.utils.EnvUtil
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/7/29
  * 反欺诈风险评分
  */
object AntiFraudScoreBYSparkSQL /*extends Logging*/{
  val sparkConf = new SparkConf().setAppName("AntiFraudScore")
  val sc = new SparkContext(sparkConf)
  val hc = new HiveContext(sc)

  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sparkConf.set("spark.rdd.compress","true")
  sparkConf.set("spark.hadoop.mapred.output.compress","true")
  sparkConf.set("spark.hadoop.mapred.output.compression.codec","true")
  sparkConf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
  sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")

  def main(args: Array[String]): Unit = {
    if(args.length!=15){
      println("请输入参数：database、table以及mysql相关参数")
      System.exit(0)
    }

    //分别传入库名、表名、mysql相关参数
    val database = args(0)
    val table = args(1)
    val path = args(2)
    val host = args(3)
    val user = args(4)
    val password = args(5)
    val port = args(6)
    val mysqlDB = args(7)
    //结果表
    val mysqlTable = args(8)
    //变量表
    val mysqlTableNew = args(9)
    //外部传入时间
    val year = args(10)
    val month = args(11)
    val day = args(12)
    //channel
    val channel = args(13)
    val envType = args(14)

    //spark sql 加工变量
    processVariable(year,month,day)

    //logWarning("start calculate ....")
    println("start calculate ....")
    //批量打分
    try{
      predictScore(database,table,path,host,user,password, port,mysqlDB,mysqlTable,mysqlTableNew,channel,envType)
    }catch {
      case ex: Exception => /*logError(ex.getMessage)*/
        println(ex.getMessage)
      //保存该批次失败的order_id,apply_time
        hc.sql(s"select order_src,apply_time from $database.$table").write
          .mode(SaveMode.Append).saveAsTable("lkl_card_score.fqz_score_fail_record")
    }

    //备份当前已处理数据，用于增量计算
    incrementCalculateBackup
  }

  //训练模型
  def trainRFModel(data: RDD[LabeledPoint], numTrees:Int, maxDepth:Int,path:String): Unit = {
    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val categoricalFeaturesInfo = Map[Int, Int]()
    //val numTrees = 30 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "variance"
    //val maxDepth = 10
    //最大分箱数，必须大于最大的离散特征值数
    val maxBins = 50
    val model = RandomForest.trainRegressor(data, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    //save the model
    model.save(sc, s"hdfs://ns1/user/luhuamin/$path/model")
  }

  //训练模型
  def trainGBDTModel(data: RDD[LabeledPoint], numTrees:Int, maxDepth:Int,path:String): Unit ={
    //train a gbdt model
    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.setNumIterations(numTrees) // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.setMaxDepth(maxDepth)
    boostingStrategy.treeStrategy.setMinInstancesPerNode(50)
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    //boostingStrategy.treeStrategy.setCategoricalFeaturesInfo( scala.collection.mutable.Map[Int, Int]())
    //boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
    val model = GradientBoostedTrees.train(data, boostingStrategy)
    //save the model
    model.save(sc, s"hdfs://ns1/user/luhuamin/$path/model")
  }

  //预测打分,并保存到mysql
  def predictScore(database:String,table:String,path:String,host:String,user:String,
         password:String, port:String,mysqlDB:String,mysqlTable:String,mysqlTableNew:String,
         channel:String,envType:String): Unit ={
    val variable = hc.sql(s"select * from $database.$table")
    //实时数据
    val dataInstance = hc.sql(s"select * from $database.$table").map {
      row =>
        val arr = new ArrayBuffer[Double]()
        //剔除label、phone字段
        for (i <- 2 until row.size) {
          if (row.get(i).isInstanceOf[Double])
            arr += row.getDouble(i)
          else if (row.get(i).isInstanceOf[Long])
            arr += row.getLong(i).toDouble
          else arr += 0.0
        }
        (row(1),row(0), Vectors.dense(arr.toArray))
    }
    //每批次总数
    val batchCnt = dataInstance.count()
    //logWarning("the count of this batch is " + batchCnt)
    println("the count of this batch is " + batchCnt)
    //加载模型，目前只考虑gbdt
    val model = GradientBoostedTreesModel.load(sc,s"hdfs://ns1/user/luhuamin/$path/model/gbdt")
    //打分数据
    val preditDataGBDT = dataInstance.map { point =>
      val prediction = model.predict(point._3)
      //order_id,apply_time,score
      (point._1,point._2, prediction)
    }
    //rdd转dataFrame
    val rowRDD = preditDataGBDT.map(row => Row(row._1.toString,row._2.toString,row._3.toString))
    val schema = StructType(
      List(
        StructField("order_id", StringType, true),
        StructField("apply_time", StringType, true),
        StructField("score", StringType, true)
      )
    )
    //将RDD映射到rowRDD，schema信息应用到rowRDD上
    val scoreDataFrame = hc.createDataFrame(rowRDD,schema)

    //保存结果至mysql和hiv
    //logWarning(" load to mysql success! 该批次总数" + batchCnt)
    println(" load to hive ! 该批次总数" + batchCnt)
    FS2Hive(scoreDataFrame,"fqz_score_result")
    println(" load to mysql ! 该批次总数" + batchCnt)
    FS2JDBC(model,scoreDataFrame,host,user,password,port,mysqlDB,mysqlTable)

    //保存变量至mysql和hive
    println(" variable load to hive ! 该批次总数" + batchCnt)
    FS2Hive(variable,"fqz_score_variable")
    println(" variable load to mysql ! 该批次总数" + batchCnt)
    FS2JDBC(model,variable,host,user,password,port,mysqlDB,mysqlTableNew)
    //logWarning(" load to hive success! 该批次总数" + batchCnt)

    //发送评分结果到redis
    println("start send scoreRsult to redis!!")
    //sendMsg2Redis(scoreDataFrame,channel,envType)
  }

  //send msg to redis
  def sendMsg2Redis(scoreDataFrame:DataFrame,channel:String,envType:String): Unit ={
    try{
      //create jedis
      val jedis: JedisCluster = EnvUtil.jedisCluster(envType)
      println("start send msg to redis")
      jedis.publish(channel,scoreDataFrame.toJSON.toString())
      println("---------"+scoreDataFrame.toJSON.toString())
      println("send msg to redis success !!")
    }catch{  case ex: Exception => /*logError(ex.getMessage)*/ println(ex.getMessage)
      //logError("FS2JDBC异常。。。")
      println("send msg to redis exception!!!")
    }
  }

  //load to mysql
  def FS2JDBC(model:GradientBoostedTreesModel,dataInstance:DataFrame,host:String,user:String,password:String,
              port:String,mysqlDB:String,mysqlTable:String): Unit ={
     try{
        val url = s"jdbc:mysql://$host:$port/$mysqlDB?user=$user&password=$password&useUnicode=true&characterEncoding=utf-8&useSSL=false&autoReconnect=true&failOverReadOnly=false"
        dataInstance.write.mode(SaveMode.Append).jdbc(url,mysqlTable,new Properties())
        //考虑异常处理
     }catch{
       case ex: Exception => /*logError(ex.getMessage)*/ println(ex.getMessage)
       //logError("FS2JDBC异常。。。")
       println("FS2JDBC异常。。。")
     }
  }

  //load to hive
  def FS2Hive(dataInstance:DataFrame,hiveTable:String): Unit ={
    try{
      dataInstance.write.mode(SaveMode.Append).saveAsTable(s"lkl_card_score.$hiveTable")
    }catch{
      case ex: Exception => /*logError(ex.getMessage)*/ println(ex.getMessage)
        //logError("FS2Hive异常。。。")
        println("FS2Hive异常。。。")
    }
  }

  //spark sql加工变量
  def processVariable(year:String,month:String,day:String): Unit ={
    hc.sql("use lkl_card_score")
    //01_0a_fqz_order_related_graph.sql
    /*hc.sql(s"insert overwrite table fqz_order_related_graph_current partition(year='${year}', month='${month}',day='${day}')\n" +
      s"select\n'1' as degree_type,\na.c0 as order_src,\na.c6 as cert_no_src,\n" +
      s"a.c5 as apply_time_src,\na.c1,a.c2,a.c3,\na.c4 as order1,\nc.performance as performance1,\n" +
      s"c.apply_time as apply_time1,\nc.type as type1,\nc.history_due_day as history_due_day1,\n" +
      s"c.current_due_day as current_due_day1,\nc.cert_no as cert_no1,\nc.label as label1,\n" +
      s"'null' as c5,\n'null' as c6,\n'null' as c7,\n'null' as order2,\n'null' as performance2,\n" +
      s"'null' as apply_time2,\n'null' as type2,\n0 as history_due_day2,\n0 as current_due_day2,\n" +
      s"'null' as cert_no2,\n0 as label2\nfrom one_degree_data a   \nj" +
      s"oin fqz_order_performance_data_new c on a.c4 = c.order_id\n" +
      s"join graphx_tansported_ordernos d on a.c0 = d.c0\n" +
      s"where a.year = ${year} and a.month = ${month} and a.day = ${day}\n" +
      s"and c.year = ${year} and c.month = ${month} and c.day = ${day}\n" +
      s"and d.year = ${year} and d.month = ${month} and d.day = ${day} \n" +
      s"union all\nselect \n'2' as degree_type,\na.c0 as order_src,\na.c10 as cert_no_src,\n" +
      s"a.c9 as apply_time_src,\na.c1,a.c2,a.c3,\na.c4 as order1,\nc.performance as performance1,\n" +
      s"c.apply_time as apply_time1,\nc.type as type1,\nc.history_due_day as history_due_day1,\n" +
      s"c.current_due_day as current_due_day1,\nc.cert_no as cert_no1,\nc.label as label1,\n" +
      s"a.c5,a.c6,a.c7,\na.c8 as order2,\nd.performance as performance2,\nd.apply_time as apply_time2,\n" +
      s"d.type as type2,\nd.history_due_day as history_due_day2,\nd.current_due_day as current_due_day2,\n" +
      s"d.cert_no as cert_no2,\nd.label as label2\nfrom two_degree_data a  \n" +
      s"join fqz_order_performance_data_new c on a.c4 = c.order_id\n" +
      s"join fqz_order_performance_data_new d on a.c8 = d.order_id\n" +
      s"join graphx_tansported_ordernos e on a.c0 = e.c0\n" +
      s"where a.year = ${year} and a.month = ${month} and a.day = ${day}\n"+
      s"and c.year = ${year} and c.month = ${month} and c.day = ${day}\n" +
      s"and d.year = ${year} and d.month = ${month} and d.day = ${day}\n" +
      s"and e.year = ${year} and e.month = ${month} and e.day = ${day}\n")

    //01_0b_fqz_order_related_graph.sql
    hc.sql(s"drop table fqz_order_data_inc \n")
    hc.sql(s"create table fqz_order_data_inc as\n" +
      s"select a.* from fqz_order_related_graph_current a\n" +
      s"left join fqz_order_related_graph_current_history b on a.order_src = b.order_src\n" +
      s"where b.order_src is null \n" +
      s"and a.year = ${year} and a.month = ${month} and a.day = ${day}\n")
    hc.sql(s"insert overwrite table fqz_order_related_graph_current_history\n" +
      s"select a.* from fqz_order_related_graph_current a \n" +
      s"where a.year = ${year} and a.month = ${month} and a.day = ${day}")*/

    //调整逻辑，提前过滤增量子图
    //01_0a_fqz_order_related_graph.sql
    hc.sql(s"drop table graphx_tansported_ordernos_current\n")
    hc.sql(s"create table graphx_tansported_ordernos_current as\n" +
      s"select * from graphx_tansported_ordernos a\n" +
      s"where a.year = ${year} and a.month = ${month} and a.day = ${day}")
    hc.sql("drop table graphx_tansported_ordernos_inc")
    hc.sql(s"create table graphx_tansported_ordernos_inc as\n" +
      s"select a.* from graphx_tansported_ordernos_current a\n" +
      s"left join graphx_tansported_ordernos_history b on a.c0 = b.c0\n" +
      s"where b.c0 is null")
    //避免增量计算缺失数据，数据备份在任务结束后进行
    /*hc.sql("drop table graphx_tansported_ordernos_history")
    hc.sql("create table graphx_tansported_ordernos_history as \n" +
      "select * from graphx_tansported_ordernos_current")*/

    //01_0b_fqz_order_related_graph_20170808.sql
    hc.sql("drop table fqz_order_data_inc")
    hc.sql(s"create table fqz_order_data_inc as \n" +
      s"select\n'1' as degree_type,\na.c0 as order_src,\na.c6 as cert_no_src,\n" +
      s"a.c5 as apply_time_src,\na.c1,a.c2,a.c3,\na.c4 as order1,\nc.performance as performance1,\n" +
      s"c.apply_time as apply_time1,\nc.type as type1,\nc.history_due_day as history_due_day1,\n" +
      s"c.current_due_day as current_due_day1,\nc.cert_no as cert_no1,\nc.label as label1,\n" +
      s"'null' as c5,\n'null' as c6,\n'null' as c7,\n'null' as order2,\n'null' as performance2,\n" +
      s"'null' as apply_time2,\n'null' as type2,\n0 as history_due_day2,\n0 as current_due_day2,\n" +
      s"'null' as cert_no2,\n0 as label2\nfrom one_degree_data a   \nj" +
      s"oin fqz_order_performance_data_new c on a.c4 = c.order_id\n" +
      s"join graphx_tansported_ordernos_inc d on a.c0 = d.c0\n" +
      s"where a.year = ${year} and a.month = ${month} and a.day = ${day}\n" +
      s"and c.year = ${year} and c.month = ${month} and c.day = ${day}\n" +
      s"and d.year = ${year} and d.month = ${month} and d.day = ${day} \n" +
      s"union all\nselect \n'2' as degree_type,\na.c0 as order_src,\na.c10 as cert_no_src,\n" +
      s"a.c9 as apply_time_src,\na.c1,a.c2,a.c3,\na.c4 as order1,\nc.performance as performance1,\n" +
      s"c.apply_time as apply_time1,\nc.type as type1,\nc.history_due_day as history_due_day1,\n" +
      s"c.current_due_day as current_due_day1,\nc.cert_no as cert_no1,\nc.label as label1,\n" +
      s"a.c5,a.c6,a.c7,\na.c8 as order2,\nd.performance as performance2,\nd.apply_time as apply_time2,\n" +
      s"d.type as type2,\nd.history_due_day as history_due_day2,\nd.current_due_day as current_due_day2,\n" +
      s"d.cert_no as cert_no2,\nd.label as label2\nfrom two_degree_data a  \n" +
      s"join fqz_order_performance_data_new c on a.c4 = c.order_id\n" +
      s"join fqz_order_performance_data_new d on a.c8 = d.order_id\n" +
      s"join graphx_tansported_ordernos_inc e on a.c0 = e.c0\n" +
      s"where a.year = ${year} and a.month = ${month} and a.day = ${day}\n"+
      s"and c.year = ${year} and c.month = ${month} and c.day = ${day}\n" +
      s"and d.year = ${year} and d.month = ${month} and d.day = ${day}\n" +
      s"and e.year = ${year} and e.month = ${month} and e.day = ${day}\n")

    //02_0a_overdue_data.sql
    hc.sql(s"drop table overdue_cnt_self_instant\n")
    hc.sql(s"create table overdue_cnt_self_instant as \n-- 一度关联自身_订单数量     \n" +
      s"SELECT a.order_src,'order_cnt' title, \ncount(distinct a.order1) cnt \n" +
      s"FROM fqz_order_data_inc a     --order订单表现\n" +
      s"where a.degree_type='1' \nand a.apply_time_src>a.apply_time1\n" +
      s"and a.cert_no_src = a.cert_no1\ngroup by  a.order_src    \n" +
      s"union all \n-- 一度关联自身_ID数量  \n" +
      s"SELECT a.order_src,'id_cnt' title,count(distinct a.cert_no1) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现\n" +
      s"where a.degree_type='1' and a.apply_time_src>a.apply_time1\n" +
      s"and a.cert_no_src = a.cert_no1\ngroup by a.order_src \n" +
      s"union all\n-- 一度关联自身_黑合同数量\n" +
      s"SELECT a.order_src,'black_cnt' title,count(distinct a.order1) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现\n" +
      s"where a.degree_type='1' and a.apply_time_src>a.apply_time1\n" +
      s"and a.cert_no_src = a.cert_no1 and a.label1 = 1\n" +
      s"group by a.order_src\n-- 一度关联自身_Q标拒绝数量  1  \n" +
      s"union all \n" +
      s"SELECT a.order_src,'q_refuse_cnt' title,count(distinct a.order1) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现  \n" +
      s"where performance1='q_refuse' and a.degree_type='1' and a.apply_time_src>a.apply_time1\n" +
      s"and a.cert_no_src = a.cert_no1\ngroup by a.order_src \n" +
      s"union all\n-- 一度关联自身_通过合同数量 \n" +
      s"SELECT a.order_src,'pass_cnt' title,count(distinct a.order1) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现\n" +
      s"where a.type1='pass'  and a.degree_type='1' and a.apply_time_src>a.apply_time1\n" +
      s"and a.cert_no_src = a.cert_no1\ngroup by a.order_src \n" +
      s"--合并数据  一度关联自身\n" )
      hc.sql(s"drop table  lkl_card_score.overdue_cnt_self_sum_instant\n" )
      hc.sql(s"create table  lkl_card_score.overdue_cnt_self_sum_instant  as\n" +
      s"select order_src,\nsum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt_self , \n" +
      s"sum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt_self ,   \n" +
      s"sum(case when title= 'black_cnt' then cnt else 0 end ) black_cnt_self , \n" +
      s"sum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt_self,    \n" +
      s"sum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt_self \n" +
      s"from  overdue_cnt_self_instant\ngroup by order_src")

    //02_0b_overdue_data.sql
    hc.sql(s"drop table overdue_cnt_2_self_tmp_instant\n" )
      hc.sql(s"create table overdue_cnt_2_self_tmp_instant as \n" +
      s"select c.order_src,\n        'overdue0' title\n       ,count(distinct c.order1) cnt \n " +
      s"      from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n " +
      s"  and c.current_due_day1<=0  --当前\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n" +
      s"   --一度关联自身\n   and c.cert_no_src = c.cert_no1\n   group by c.order_src\n" +
      s"union all\nselect c.order_src,\n        'overdue3' title\n       ,count(distinct c.order1) cnt \n " +
      s"      from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n" +
      s"   and c.current_due_day1>3  --当前\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1  \n" +
      s"   --一度关联自身\n   and c.cert_no_src = c.cert_no1\n  group by c.order_src\n" +
      s"union all\nselect c.order_src,\n        'overdue30' title\n       ,count(distinct c.order1) cnt \n " +
      s"      from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n" +
      s"   and c.current_due_day1>30 --当前\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n" +
      s"   --一度关联自身\n   and c.cert_no_src = c.cert_no1\n  group by c.order_src \n" +
      s"union all\n--历史逾期\nselect c.order_src,\n        'overdue0_ls' title\n" +
      s"       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n" +
      s" where c.type1='pass'       --通过 \n   and c.history_due_day1<=0  --历史\n " +
      s"  and c.degree_type='1' and c.apply_time_src>c.apply_time1\n   --一度关联自身\n " +
      s"  and c.cert_no_src = c.cert_no1   \n  group by c.order_src\nunion all\n" +
      s"select c.order_src,\n        'overdue3_ls' title\n       ,count(distinct c.order1) cnt \n" +
      s"       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n" +
      s"   and c.history_due_day1>3 --历史\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n " +
      s"  --一度关联自身\n   and c.cert_no_src = c.cert_no1   \n  group by c.order_src\n" +
      s"union all\nselect c.order_src,\n        'overdue30_ls' title\n " +
      s"      ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n" +
      s" where c.type1='pass'       --通过 \n   and c.history_due_day1>30  --历史\n " +
      s"  and c.degree_type='1' and c.apply_time_src>c.apply_time1  \n   --一度关联自身\n " +
      s"  and c.cert_no_src = c.cert_no1\n   group by c.order_src\n" +
      s"--合并一度关联 逾期数据(关联自身)\n" )
     hc.sql(s"drop table  lkl_card_score.overdue_cnt_2_self_instant\n" )
      hc.sql(s"create table  lkl_card_score.overdue_cnt_2_self_instant  as\n" +
      s"select order_src,\nsum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           \n" +
      s"sum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   \n" +
      s"sum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  \n" +
      s"sum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, \n" +
      s"sum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,\n" +
      s"sum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls \n" +
      s"from  overdue_cnt_2_self_tmp_instant\ngroup by order_src")

    //02_0c_overdue_data.sql
    hc.sql(s"--一度关联排除自身   \ndrop table overdue_cnt_1_instant\n" )
     hc.sql(s"create table overdue_cnt_1_instant as \n-- 一度_订单数量  4  \n" +
      s"SELECT a.order_src,'order_cnt' title, \ncount(distinct a.order1) cnt \n" +
      s"FROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='1' \n" +
      s"and a.apply_time_src>a.apply_time1\n--一度关联排除自身\nand a.cert_no_src <> a.cert_no1\n" +
      s"group by  a.order_src    \nunion all \n-- 一度_ID数量    \n" +
      s"SELECT a.order_src,'id_cnt' title,count(distinct a.cert_no1) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现\n" +
      s"where a.degree_type='1' and a.apply_time_src>a.apply_time1\n" +
      s"--一度关联排除自身\nand a.cert_no_src <> a.cert_no1\n" +
      s"group by a.order_src \nunion all\n-- 一度关联自身_黑合同数量\n" +
      s"SELECT a.order_src,'black_cnt' title,count(distinct a.order1) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='1' " +
      s"and a.apply_time_src>a.apply_time1\nand a.cert_no_src <> a.cert_no1 and a.label1 = 1\n" +
      s"group by a.order_src\n-- 一度_Q标拒绝数量  1  \nunion all \n" +
      s"SELECT a.order_src,'q_refuse_cnt' title,count(distinct a.order1) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现  \n" +
      s"where performance1='q_refuse' and a.degree_type='1' and a.apply_time_src>a.apply_time1\n" +
      s"--一度关联排除自身\nand a.cert_no_src <> a.cert_no1\ngroup by a.order_src \n" +
      s"union all\n-- 一度_通过合同数量  2  \n" +
      s"SELECT a.order_src,'pass_cnt' title,count(distinct a.order1) cnt FROM fqz_order_data_inc a" +
      s"     --order订单表现\nwhere a.type1='pass'  and a.degree_type='1' " +
      s"and a.apply_time_src>a.apply_time1\n--一度关联排除自身\nand a.cert_no_src <> a.cert_no1\n" +
      s"group by a.order_src " )
     hc.sql(s"\ndrop table  lkl_card_score.overdue_cnt_1_sum_instant\n" )
     hc.sql(s"create table  lkl_card_score.overdue_cnt_1_sum_instant  as\nselect order_src,\n" +
      s"sum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt ,           \n" +
      s"sum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt ,\n" +
      s"sum(case when title= 'black_cnt' then cnt else 0 end ) black_cnt ,    \n" +
      s"sum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt ,    \n" +
      s"sum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt \nfrom  overdue_cnt_1_instant\n" +
      s"group by order_src")

    //02_0d_overdue_data.sql
    hc.sql(s"drop table overdue_cnt_2_tmp_instant\n" )
    hc.sql(s"create table overdue_cnt_2_tmp_instant as \nselect c.order_src,\n" +
      s"        'overdue0' title\n       ,count(distinct c.order1) cnt \n       " +
      s"from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   " +
      s"and c.current_due_day1<=0  --当前\n   and c.degree_type='1' " +
      s"and c.apply_time_src>c.apply_time1\n   --一度关联排除自身\n   " +
      s"and c.cert_no_src <> c.cert_no1\n   group by c.order_src\n" +
      s"union all\nselect c.order_src,\n        'overdue3' title\n       ,count(distinct c.order1) cnt \n" +
      s"       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   " +
      s"and c.current_due_day1>3 --当前 \n   and c.degree_type='1' and c.apply_time_src>c.apply_time1  \n" +
      s"   --一度关联排除自身\n   and c.cert_no_src <> c.cert_no1\n  group by c.order_src\nunion all\n" +
      s"select c.order_src,\n        'overdue30' title\n       ,count(distinct c.order1) cnt \n" +
      s"       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n" +
      s"   and c.current_due_day1>30 --当前\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n" +
      s"   --一度关联排除自身\n   and c.cert_no_src <> c.cert_no1\n  group by c.order_src \n" +
      s"union all\n--历史逾期\nselect c.order_src,\n        'overdue0_ls' title\n" +
      s"       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n" +
      s" where c.type1='pass'       --通过 \n   and c.history_due_day1<=0  --历史\n" +
      s"   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n   --一度关联排除自身\n" +
      s"   and c.cert_no_src <> c.cert_no1   \n  group by c.order_src\nunion all\nselect c.order_src,\n" +
      s"        'overdue3_ls' title\n       ,count(distinct c.order1) cnt \n       from \n" +
      s"   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   and c.history_due_day1>3 --历史\n " +
      s"  and c.degree_type='1' and c.apply_time_src>c.apply_time1\n   --一度关联排除自身\n" +
      s"   and c.cert_no_src <> c.cert_no1   \n  group by c.order_src\nunion all\nselect c.order_src,\n " +
      s"       'overdue30_ls' title\n       ,count(distinct c.order1) cnt \n       from \n" +
      s"   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n " +
      s"  and c.history_due_day1>30  --历史\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1  \n" +
      s"   --一度关联排除自身\n   and c.cert_no_src <> c.cert_no1\n   group by c.order_src\n " +
      s"  \n--合并一度关联 逾期数据(排除自身)\n" )
    hc.sql(s"drop table  lkl_card_score.overdue_cnt_2_instant\n" )
    hc.sql(s"create table  lkl_card_score.overdue_cnt_2_instant  as\nselect order_src,\n" +
      s"sum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           \n" +
      s"sum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   \n" +
      s"sum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  \n" +
      s"sum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, \n" +
      s"sum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,\n" +
      s"sum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls \n" +
      s"from  overdue_cnt_2_tmp_instant\n" +
      s"group by order_src")

    //02_0e_overdue_data.sql
      hc.sql(s"drop table overdue_cnt_1_2_instant\n" )
      hc.sql(s"create table overdue_cnt_1_2_instant as \n-- 2度_订单数量     \n" +
      s"SELECT a.order_src,'order_cnt' title, \ncount(distinct a.order2) cnt \n" +
      s"FROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='2' \n" +
      s"and a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前\n" +
      s"and a.apply_time_src>a.apply_time1\ngroup by  a.order_src    \n" +
      s"union all \n-- 2度_ID数量     \nSELECT a.order_src,'id_cnt' title,\n" +
      s"count(distinct a.cert_no2) cnt \nFROM fqz_order_data_inc a     --order订单表现\n" +
      s"where a.degree_type='2' \n" +
      s"and a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前\n" +
      s"and a.apply_time_src>a.apply_time1\ngroup by a.order_src \n" +
      s"union all\n-- 2度_黑合同数量\n" +
      s"SELECT a.order_src,'black_cnt' title,count(distinct a.order2) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='2' \n" +
      s"and a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前 \n" +
      s"and a.apply_time_src>a.apply_time1\nand a.label2 = 1\ngroup by a.order_src\n" +
      s"-- 2度_Q标拒绝数量  2  \nunion all \nSELECT a.order_src,'q_refuse_cnt' title,\n" +
      s"count(distinct a.order2) cnt \nFROM fqz_order_data_inc a     --order订单表现  \n" +
      s"where performance2='q_refuse' \nand a.degree_type='2' \n" +
      s"and a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前 \n" +
      s"and a.apply_time_src>a.apply_time1\ngroup by a.order_src \nunion all\n" +
      s"-- 2度_通过合同数量  2  \nSELECT a.order_src,'pass_cnt' title,\n" +
      s"count(distinct a.order2) cnt \nFROM fqz_order_data_inc a     --order订单表现\n" +
      s"where a.type2='pass'  \nand a.degree_type='2' \n" +
      s"and a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前\n" +
      s"and a.apply_time_src>a.apply_time1\ngroup by a.order_src " +
      s"\n--合并二度关联数据 \n" )
        hc.sql(s"drop table overdue_cnt_2_sum_instant\n" )
        hc.sql(s"create table  lkl_card_score.overdue_cnt_2_sum_instant  as\n" +
      s"select order_src,\nsum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt ,           \n" +
      s"sum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt ,   \n" +
      s"sum(case when title= 'black_cnt' then cnt else 0 end ) black_cnt , \n" +
      s"sum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt ,    \n" +
      s"sum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt \nfrom  overdue_cnt_1_2_instant\n" +
      s"group by order_src")

    //02_0f_overdue_data.sql
    hc.sql(s"\ndrop table overdue_cnt_2_2_tmp_instant\n" )
      hc.sql(s"create table overdue_cnt_2_2_tmp_instant as \nselect c.order_src,\n" +
      s"        'overdue0' title\n       ,count(distinct c.order2) cnt \n       from \n" +
      s"   fqz_order_data_inc c \n where c.type2='pass'       --通过 \n" +
      s"   and c.current_due_day2<=0  --当前\n   and c.degree_type='2' \n" +
      s"   and c.apply_time_src>c.apply_time1  \n   and c.apply_time_src>c.apply_time2 \n" +
      s"   group by c.order_src\nunion all\nselect c.order_src,\n        'overdue3' title\n " +
      s"      ,count(distinct c.order2) cnt \n       from \n   fqz_order_data_inc c \n" +
      s" where c.type2='pass'       --通过 \n   and c.current_due_day2>3 --当前 \n" +
      s"   and c.degree_type='2' and c.apply_time_src>c.apply_time1 \n" +
      s"   and c.apply_time_src>c.apply_time2    \n  group by c.order_src\nunion all\n" +
      s"select c.order_src,\n        'overdue30' title\n       ,count(distinct c.order2) cnt \n" +
      s"       from \n   fqz_order_data_inc c \n where c.type2='pass'       --通过 \n" +
      s"   and c.current_due_day2>30  --当前\n   and c.degree_type='2' and c.apply_time_src>c.apply_time1 \n" +
      s"   and c.apply_time_src>c.apply_time2 \n  group by c.order_src \nunion all\nselect c.order_src,\n" +
      s"        'overdue0_ls' title\n       ,count(distinct c.order2) cnt \n       from \n" +
      s"   fqz_order_data_inc c \n where c.type2='pass'       --通过 \n " +
      s"  and c.history_due_day2<=0 --历史\n   and c.degree_type='2' and c.apply_time_src>c.apply_time1 \n" +
      s"   and c.apply_time_src>c.apply_time2    \n  group by c.order_src\nunion all\nselect c.order_src,\n" +
      s"        'overdue3_ls' title\n       ,count(distinct c.order2) cnt \n       from \n" +
      s"   fqz_order_data_inc c \n where c.type2='pass'       --通过 \n " +
      s"  and c.history_due_day2> 3 --历史\n   and c.degree_type='2' and c.apply_time_src>c.apply_time1  \n" +
      s"   and c.apply_time_src>c.apply_time2 \n  group by c.order_src\nunion all\nselect c.order_src,\n" +
      s"        'overdue30_ls' title\n       ,count(distinct c.order2) cnt \n       from \n" +
      s"   fqz_order_data_inc c \n where c.type2='pass'       --通过  \n " +
      s"  and c.history_due_day2> 30 --历史\n   and c.degree_type='2' and c.apply_time_src>c.apply_time1 \n" +
      s"   and c.apply_time_src>c.apply_time2    \n   group by c.order_src\n   \n" +
      s"--合并二度关联 逾期数据\n" )
        hc.sql(s"drop table  lkl_card_score.overdue_cnt_2_2_instant\n" )
        hc.sql(s"create table  lkl_card_score.overdue_cnt_2_2_instant  as\nselect order_src,\n" +
      s"sum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           \n" +
      s"sum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   \n" +
      s"sum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  \n" +
      s"sum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, \n" +
      s"sum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,\n" +
      s"sum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls \n" +
      s"from  overdue_cnt_2_2_tmp_instant\n" +
      s"group by order_src")

    //03_overdue_result_all.sql
    hc.sql(s"drop table order_src_group_instant\n" )
      hc.sql(s"create table order_src_group_instant as  \nselect trim(c0) order_src   \n" +
      s"from graphx_tansported_ordernos_inc\ngroup by c0 \n--宽表合并\n" )
        hc.sql(s"drop table overdue_result_all_instant\n" )
        hc.sql(s"create table overdue_result_all_instant as \n" +
      s"select \na.order_src,\nnvl(b.order_cnt_self,0) as order_cnt_self,\n" +
      s"nvl(b.id_cnt_self,0) as id_cnt_self,\nnvl(b.black_cnt_self,0) as black_cnt_self,\n" +
      s"nvl(b.q_refuse_cnt_self,0) as q_refuse_cnt_self,\nnvl(b.pass_cnt_self,0) as pass_cnt_self,\n" +
      s"nvl(c.overdue0,0) as overdue0_self,\nnvl(c.overdue3,0) as overdue3_self,\n" +
      s"nvl(c.overdue30,0) as overdue30_self,\nnvl(c.overdue0_ls,0) as overdue0_ls_self,\n" +
      s"nvl(c.overdue3_ls,0) as overdue3_ls_self,\nnvl(c.overdue30_ls,0) as overdue30_ls_self,\n" +
      s"nvl(d.order_cnt,0) as order_cnt,\nnvl(d.id_cnt,0) as id_cnt,\n" +
      s"nvl(d.black_cnt,0) as black_cnt,\nnvl(d.q_refuse_cnt,0) as q_refuse_cnt,\n" +
      s"nvl(d.pass_cnt,0) as pass_cnt,\nnvl(e.overdue0,0) as overdue0,\nnvl(e.overdue3,0) as overdue3,\n" +
      s"nvl(e.overdue30,0) as overdue30,\nnvl(e.overdue0_ls,0) as overdue0_ls ,\n" +
      s"nvl(e.overdue3_ls,0) as overdue3_ls,\nnvl(e.overdue30_ls,0) as overdue30_ls, \n" +
      s"nvl(f.order_cnt,0) as order_cnt_2,\nnvl(f.id_cnt,0)       as id_cnt_2,\n" +
      s"nvl(f.black_cnt,0)       as black_cnt_2,\nnvl(f.q_refuse_cnt,0) as q_refuse_cnt_2,\n" +
      s"nvl(f.pass_cnt,0)    as pass_cnt_2,\nnvl(g.overdue0,0)    as overdue0_2,\n" +
      s"nvl(g.overdue3,0)    as overdue3_2,\nnvl(g.overdue30,0)   as overdue30_2,\n" +
      s"nvl(g.overdue0_ls,0) as overdue0_ls_2,\nnvl(g.overdue3_ls,0) as overdue3_ls_2,\n" +
      s"nvl(g.overdue30_ls,0) as overdue30_ls_2\nfrom order_src_group_instant a   --256441\n" +
      s"left join overdue_cnt_self_sum_instant b on a.order_src = b.order_src  " +
      s"-- 一度关联自身（订单数量、ID数量、黑合同数量、Q标拒绝数量 ）\n" +
      s"left join overdue_cnt_2_self_instant c on a.order_src = c.order_src    " +
      s"-- 合并一度关联 逾期数据(关联自身)\n" +
      s"left join overdue_cnt_1_sum_instant d on a.order_src = d.order_src     " +
      s"-- 一度关联排除自身（订单数量、ID数量、黑合同数量、Q标拒绝数量 ）\n" +
      s"left join overdue_cnt_2_instant  e on a.order_src = e.order_src        " +
      s"-- 一度关联 逾期数据(排除自身)\nleft join overdue_cnt_2_sum_instant f on a.order_src = f.order_src" +
      s"     -- 二度关联数据 （订单数量、ID数量、黑合同数量、Q标拒绝数量 ）\n" +
      s"left join overdue_cnt_2_2_instant g on a.order_src = g.order_src      " +
      s"-- 二度关联 逾期数据\n--数据处理，过滤全0值，是否过滤\n" )

    //保留变量值为0的情况，便于反馈统计
    /*hc.sql(s"insert overwrite table overdue_result_all_instant \n" +
      s"select * from overdue_result_all_instant\nwhere !(\n  order_cnt_self         = 0 and \n" +
      s"  id_cnt_self           = 0 and \n  black_cnt_self       = 0 and\n  q_refuse_cnt_self     = 0 " +
      s"and \n  pass_cnt_self         = 0 and \n  overdue0_self         = 0 and \n  " +
      s"overdue3_self         = 0 and \n  overdue30_self         = 0 and \n  " +
      s"overdue0_ls_self      = 0 and \n  overdue3_ls_self      = 0 and \n " +
      s" overdue30_ls_self     = 0 and \n  order_cnt           = 0 and \n  " +
      s"id_cnt               = 0 and \n  black_cnt            = 0 and\n  q_refuse_cnt         = 0 and \n" +
      s"  pass_cnt           = 0 and \n  overdue0           = 0 and \n  overdue3           = 0 and \n" +
      s"  overdue30           = 0 and \n  overdue0_ls           = 0 and \n " +
      s" overdue3_ls           = 0 and \n  overdue30_ls         = 0 and \n  " +
      s"order_cnt_2           = 0 and \n  id_cnt_2           = 0 and\n  black_cnt_2            = 0 and \n" +
      s"  q_refuse_cnt_2         = 0 and \n  pass_cnt_2           = 0 and \n " +
      s" overdue0_2           = 0 and \n  overdue3_2           = 0 and \n  overdue30_2           = 0 and \n" +
      s"  overdue0_ls_2         = 0 and \n  overdue3_ls_2         = 0 and \n " +
      s" overdue30_ls_2         = 0\n)")*/

    //04_overdue_result_all.sql
    hc.sql(s"drop table order_src_bian_tmp_instant\n" )
      hc.sql(s"create table order_src_bian_tmp_instant as \nselect \na.order_src,\n" +
      s"concat(a.c1,'|',a.c3) as ljmx,\n1 as depth \nfrom fqz_order_data_inc a\n" +
      s"where a.degree_type = '1' \nand a.apply_time_src>a.apply_time1 \n--一度关联进件为黑\n" +
      s"and a.label1 = 1 \nunion all \nselect \na.order_src,\n" +
      s"concat(a.c1,'|',a.c3,'|',a.c5,'|',a.c7) as ljmx,\n2 as depth \n" +
      s"from fqz_order_data_inc a\nwhere a.degree_type = '2' \nand a.apply_time_src>a.apply_time1 \n" +
      s"and a.apply_time_src>a.apply_time2    \n--二度关联进件为黑\nand a.label2 = 1\n" +
      s"--===========聚合关联边\n" )
        hc.sql(s"drop table order_src_bian_instant   \n" )
        hc.sql(s"create table order_src_bian_instant as \nselect c.order_src," +
      s"concat_ws(',',collect_set(ljmx)) as ljmx           \n" +
      s"from  order_src_bian_tmp_instant  c\ngroup by c.order_src\n\n" +
      s"--边字段 关联到结果表 \n" )
        hc.sql(s"drop table overdue_result_all_new_instant\n" )
        hc.sql(s"create table overdue_result_all_new_instant as \nselect \n--c.label,\n" +
      s"tab.c1 as apply_time,\na.*,b.ljmx\nfrom overdue_result_all_instant a\n" +
      s"left join order_src_bian_instant b on a.order_src=b.order_src\njoin \n" +
      s"(select c0,c1 from graphx_tansported_ordernos_inc c \nwhere year = ${year} " +
      s"and month = ${month} and day = ${day}\ngroup by c0,c1) tab " +
      s"on a.order_src = tab.c0")

    //05_overdue_result_all_new_woe.sql
    hc.sql(s"drop table fqz_edge_depth_instant\n" )
      hc.sql(s"create table fqz_edge_depth_instant as\n" +
      s"select order_src,max(depth) as depth from order_src_bian_tmp_instant \n" +
      s"group by order_src\n\n--统计每个订单边权重 , 每个边woe依赖全量统计\n" )
     hc.sql(s"drop table fqz_order_edge_woe_instant\n" )
     hc.sql(s"create table fqz_order_edge_woe_instant as \n" +
      s"select \na.order_src,\nsum(b.woe) as edge_woe_sum,\nmax(woe) as edge_woe_max,\n" +
      s"min(woe) as edge_woe_min\nfrom \nfqz_edge_data_total_instant a \n" +
      s"join fqz_edge_woe b on a.edge = b.edge   --每个边woe值依赖全局统计\n" +
      s"group by a.order_src\n\n--合并最总结果\n" )
        hc.sql(s"drop table overdue_result_all_new_woe_instant\n" )
          hc.sql(s"create table overdue_result_all_new_woe_instant as\n" +
      s"select a.*,\nb.edge_woe_sum,\nb.edge_woe_max,\nb.edge_woe_min,\n" +
      s"c.depth\nfrom overdue_result_all_new_instant a \n" +
      s"left join fqz_order_edge_woe_instant b on a.order_src = b.order_src\n" +
      s"left join fqz_edge_depth_instant c on a.order_src = c.order_src")
  }

  //spark sql计算变量，使用registerTempTable方式
  def processVariableByTempTable(year:String,month:String,day:String): Unit ={
    hc.sql("use lkl_card_score")
    //调整逻辑，提前过滤增量子图
    //01_0a_fqz_order_related_graph.sql
    hc.sql(s"select * from graphx_tansported_ordernos a\n" +
      s"where a.year = ${year} and a.month = ${month} and a.day = ${day}")
      .registerTempTable("graphx_tansported_ordernos_current")
    hc.sql(s"select a.* from graphx_tansported_ordernos_current a\n" +
      s"left join graphx_tansported_ordernos_history b on a.c0 = b.c0\n" +
      s"where b.c0 is null").registerTempTable("graphx_tansported_ordernos_inc")
    hc.sql("select * from graphx_tansported_ordernos_current").registerTempTable("graphx_tansported_ordernos_history")

    //缓存数据
    hc.sql("catch table one_degree_data")
    //01_0b_fqz_order_related_graph_20170808.sql
    hc.sql(s"select\n'1' as degree_type,\na.c0 as order_src,\na.c6 as cert_no_src,\n" +
      s"a.c5 as apply_time_src,\na.c1,a.c2,a.c3,\na.c4 as order1,\nc.performance as performance1,\n" +
      s"c.apply_time as apply_time1,\nc.type as type1,\nc.history_due_day as history_due_day1,\n" +
      s"c.current_due_day as current_due_day1,\nc.cert_no as cert_no1,\nc.label as label1,\n" +
      s"'null' as c5,\n'null' as c6,\n'null' as c7,\n'null' as order2,\n'null' as performance2,\n" +
      s"'null' as apply_time2,\n'null' as type2,\n0 as history_due_day2,\n0 as current_due_day2,\n" +
      s"'null' as cert_no2,\n0 as label2\nfrom one_degree_data a   \nj" +
      s"oin fqz_order_performance_data_new c on a.c4 = c.order_id\n" +
      s"join graphx_tansported_ordernos_inc d on a.c0 = d.c0\n" +
      s"where a.year = ${year} and a.month = ${month} and a.day = ${day}\n" +
      s"and c.year = ${year} and c.month = ${month} and c.day = ${day}\n" +
      s"and d.year = ${year} and d.month = ${month} and d.day = ${day} \n" +
      s"union all\nselect \n'2' as degree_type,\na.c0 as order_src,\na.c10 as cert_no_src,\n" +
      s"a.c9 as apply_time_src,\na.c1,a.c2,a.c3,\na.c4 as order1,\nc.performance as performance1,\n" +
      s"c.apply_time as apply_time1,\nc.type as type1,\nc.history_due_day as history_due_day1,\n" +
      s"c.current_due_day as current_due_day1,\nc.cert_no as cert_no1,\nc.label as label1,\n" +
      s"a.c5,a.c6,a.c7,\na.c8 as order2,\nd.performance as performance2,\nd.apply_time as apply_time2,\n" +
      s"d.type as type2,\nd.history_due_day as history_due_day2,\nd.current_due_day as current_due_day2,\n" +
      s"d.cert_no as cert_no2,\nd.label as label2\nfrom two_degree_data a  \n" +
      s"join fqz_order_performance_data_new c on a.c4 = c.order_id\n" +
      s"join fqz_order_performance_data_new d on a.c8 = d.order_id\n" +
      s"join graphx_tansported_ordernos_inc e on a.c0 = e.c0\n" +
      s"where a.year = ${year} and a.month = ${month} and a.day = ${day}\n"+
      s"and c.year = ${year} and c.month = ${month} and c.day = ${day}\n" +
      s"and d.year = ${year} and d.month = ${month} and d.day = ${day}\n" +
      s"and e.year = ${year} and e.month = ${month} and e.day = ${day}\n").registerTempTable("fqz_order_data_inc")

    //02_0a_overdue_data.sql
    hc.sql(s"SELECT a.order_src,'order_cnt' title, \ncount(distinct a.order1) cnt \n" +
      s"FROM fqz_order_data_inc a     --order订单表现\n" +
      s"where a.degree_type='1' \nand a.apply_time_src>a.apply_time1\n" +
      s"and a.cert_no_src = a.cert_no1\ngroup by  a.order_src    \n" +
      s"union all \n-- 一度关联自身_ID数量  \n" +
      s"SELECT a.order_src,'id_cnt' title,count(distinct a.cert_no1) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现\n" +
      s"where a.degree_type='1' and a.apply_time_src>a.apply_time1\n" +
      s"and a.cert_no_src = a.cert_no1\ngroup by a.order_src \n" +
      s"union all\n-- 一度关联自身_黑合同数量\n" +
      s"SELECT a.order_src,'black_cnt' title,count(distinct a.order1) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现\n" +
      s"where a.degree_type='1' and a.apply_time_src>a.apply_time1\n" +
      s"and a.cert_no_src = a.cert_no1 and a.label1 = 1\n" +
      s"group by a.order_src\n-- 一度关联自身_Q标拒绝数量  1  \n" +
      s"union all \n" +
      s"SELECT a.order_src,'q_refuse_cnt' title,count(distinct a.order1) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现  \n" +
      s"where performance1='q_refuse' and a.degree_type='1' and a.apply_time_src>a.apply_time1\n" +
      s"and a.cert_no_src = a.cert_no1\ngroup by a.order_src \n" +
      s"union all\n-- 一度关联自身_通过合同数量 \n" +
      s"SELECT a.order_src,'pass_cnt' title,count(distinct a.order1) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现\n" +
      s"where a.type1='pass'  and a.degree_type='1' and a.apply_time_src>a.apply_time1\n" +
      s"and a.cert_no_src = a.cert_no1\ngroup by a.order_src \n" +
      s"--合并数据  一度关联自身\n" ).registerTempTable("overdue_cnt_self_instant")
    hc.sql(s"select order_src,\nsum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt_self , \n" +
      s"sum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt_self ,   \n" +
      s"sum(case when title= 'black_cnt' then cnt else 0 end ) black_cnt_self , \n" +
      s"sum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt_self,    \n" +
      s"sum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt_self \n" +
      s"from  overdue_cnt_self_instant\ngroup by order_src").registerTempTable("overdue_cnt_self_sum_instant")

    //02_0b_overdue_data.sql
    hc.sql(s"select c.order_src,\n        'overdue0' title\n       ,count(distinct c.order1) cnt \n " +
      s"      from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n " +
      s"  and c.current_due_day1<=0  --当前\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n" +
      s"   --一度关联自身\n   and c.cert_no_src = c.cert_no1\n   group by c.order_src\n" +
      s"union all\nselect c.order_src,\n        'overdue3' title\n       ,count(distinct c.order1) cnt \n " +
      s"      from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n" +
      s"   and c.current_due_day1>3  --当前\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1  \n" +
      s"   --一度关联自身\n   and c.cert_no_src = c.cert_no1\n  group by c.order_src\n" +
      s"union all\nselect c.order_src,\n        'overdue30' title\n       ,count(distinct c.order1) cnt \n " +
      s"      from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n" +
      s"   and c.current_due_day1>30 --当前\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n" +
      s"   --一度关联自身\n   and c.cert_no_src = c.cert_no1\n  group by c.order_src \n" +
      s"union all\n--历史逾期\nselect c.order_src,\n        'overdue0_ls' title\n" +
      s"       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n" +
      s" where c.type1='pass'       --通过 \n   and c.history_due_day1<=0  --历史\n " +
      s"  and c.degree_type='1' and c.apply_time_src>c.apply_time1\n   --一度关联自身\n " +
      s"  and c.cert_no_src = c.cert_no1   \n  group by c.order_src\nunion all\n" +
      s"select c.order_src,\n        'overdue3_ls' title\n       ,count(distinct c.order1) cnt \n" +
      s"       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n" +
      s"   and c.history_due_day1>3 --历史\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n " +
      s"  --一度关联自身\n   and c.cert_no_src = c.cert_no1   \n  group by c.order_src\n" +
      s"union all\nselect c.order_src,\n        'overdue30_ls' title\n " +
      s"      ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n" +
      s" where c.type1='pass'       --通过 \n   and c.history_due_day1>30  --历史\n " +
      s"  and c.degree_type='1' and c.apply_time_src>c.apply_time1  \n   --一度关联自身\n " +
      s"  and c.cert_no_src = c.cert_no1\n   group by c.order_src\n" +
      s"--合并一度关联 逾期数据(关联自身)\n" ).registerTempTable("overdue_cnt_2_self_tmp_instant")
    hc.sql(s"select order_src,\nsum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           \n" +
      s"sum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   \n" +
      s"sum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  \n" +
      s"sum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, \n" +
      s"sum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,\n" +
      s"sum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls \n" +
      s"from  overdue_cnt_2_self_tmp_instant\ngroup by order_src").registerTempTable("overdue_cnt_2_self_instant")

    //02_0c_overdue_data.sql
    hc.sql(s"SELECT a.order_src,'order_cnt' title, \ncount(distinct a.order1) cnt \n" +
      s"FROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='1' \n" +
      s"and a.apply_time_src>a.apply_time1\n--一度关联排除自身\nand a.cert_no_src <> a.cert_no1\n" +
      s"group by  a.order_src    \nunion all \n-- 一度_ID数量    \n" +
      s"SELECT a.order_src,'id_cnt' title,count(distinct a.cert_no1) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现\n" +
      s"where a.degree_type='1' and a.apply_time_src>a.apply_time1\n" +
      s"--一度关联排除自身\nand a.cert_no_src <> a.cert_no1\n" +
      s"group by a.order_src \nunion all\n-- 一度关联自身_黑合同数量\n" +
      s"SELECT a.order_src,'black_cnt' title,count(distinct a.order1) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='1' " +
      s"and a.apply_time_src>a.apply_time1\nand a.cert_no_src <> a.cert_no1 and a.label1 = 1\n" +
      s"group by a.order_src\n-- 一度_Q标拒绝数量  1  \nunion all \n" +
      s"SELECT a.order_src,'q_refuse_cnt' title,count(distinct a.order1) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现  \n" +
      s"where performance1='q_refuse' and a.degree_type='1' and a.apply_time_src>a.apply_time1\n" +
      s"--一度关联排除自身\nand a.cert_no_src <> a.cert_no1\ngroup by a.order_src \n" +
      s"union all\n-- 一度_通过合同数量  2  \n" +
      s"SELECT a.order_src,'pass_cnt' title,count(distinct a.order1) cnt FROM fqz_order_data_inc a" +
      s"     --order订单表现\nwhere a.type1='pass'  and a.degree_type='1' " +
      s"and a.apply_time_src>a.apply_time1\n--一度关联排除自身\nand a.cert_no_src <> a.cert_no1\n" +
      s"group by a.order_src " ).registerTempTable("overdue_cnt_1_instant")
    hc.sql(s"select order_src,\n" +
      s"sum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt ,           \n" +
      s"sum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt ,\n" +
      s"sum(case when title= 'black_cnt' then cnt else 0 end ) black_cnt ,    \n" +
      s"sum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt ,    \n" +
      s"sum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt \nfrom  overdue_cnt_1_instant\n" +
      s"group by order_src").registerTempTable("overdue_cnt_1_sum_instant")

    //02_0d_overdue_data.sql
    hc.sql(s"select c.order_src,\n" +
      s"        'overdue0' title\n       ,count(distinct c.order1) cnt \n       " +
      s"from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   " +
      s"and c.current_due_day1<=0  --当前\n   and c.degree_type='1' " +
      s"and c.apply_time_src>c.apply_time1\n   --一度关联排除自身\n   " +
      s"and c.cert_no_src <> c.cert_no1\n   group by c.order_src\n" +
      s"union all\nselect c.order_src,\n        'overdue3' title\n       ,count(distinct c.order1) cnt \n" +
      s"       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   " +
      s"and c.current_due_day1>3 --当前 \n   and c.degree_type='1' and c.apply_time_src>c.apply_time1  \n" +
      s"   --一度关联排除自身\n   and c.cert_no_src <> c.cert_no1\n  group by c.order_src\nunion all\n" +
      s"select c.order_src,\n        'overdue30' title\n       ,count(distinct c.order1) cnt \n" +
      s"       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n" +
      s"   and c.current_due_day1>30 --当前\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n" +
      s"   --一度关联排除自身\n   and c.cert_no_src <> c.cert_no1\n  group by c.order_src \n" +
      s"union all\n--历史逾期\nselect c.order_src,\n        'overdue0_ls' title\n" +
      s"       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n" +
      s" where c.type1='pass'       --通过 \n   and c.history_due_day1<=0  --历史\n" +
      s"   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n   --一度关联排除自身\n" +
      s"   and c.cert_no_src <> c.cert_no1   \n  group by c.order_src\nunion all\nselect c.order_src,\n" +
      s"        'overdue3_ls' title\n       ,count(distinct c.order1) cnt \n       from \n" +
      s"   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   and c.history_due_day1>3 --历史\n " +
      s"  and c.degree_type='1' and c.apply_time_src>c.apply_time1\n   --一度关联排除自身\n" +
      s"   and c.cert_no_src <> c.cert_no1   \n  group by c.order_src\nunion all\nselect c.order_src,\n " +
      s"       'overdue30_ls' title\n       ,count(distinct c.order1) cnt \n       from \n" +
      s"   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n " +
      s"  and c.history_due_day1>30  --历史\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1  \n" +
      s"   --一度关联排除自身\n   and c.cert_no_src <> c.cert_no1\n   group by c.order_src\n " +
      s"  \n--合并一度关联 逾期数据(排除自身)\n" ).registerTempTable("overdue_cnt_2_tmp_instant")
    hc.sql(s"select order_src,\n" +
      s"sum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           \n" +
      s"sum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   \n" +
      s"sum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  \n" +
      s"sum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, \n" +
      s"sum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,\n" +
      s"sum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls \n" +
      s"from  overdue_cnt_2_tmp_instant\n" +
      s"group by order_src").registerTempTable("overdue_cnt_2_instant")

    //02_0e_overdue_data.sql
    hc.sql(s"SELECT a.order_src,'order_cnt' title, \ncount(distinct a.order2) cnt \n" +
      s"FROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='2' \n" +
      s"and a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前\n" +
      s"and a.apply_time_src>a.apply_time1\ngroup by  a.order_src    \n" +
      s"union all \n-- 2度_ID数量     \nSELECT a.order_src,'id_cnt' title,\n" +
      s"count(distinct a.cert_no2) cnt \nFROM fqz_order_data_inc a     --order订单表现\n" +
      s"where a.degree_type='2' \n" +
      s"and a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前\n" +
      s"and a.apply_time_src>a.apply_time1\ngroup by a.order_src \n" +
      s"union all\n-- 2度_黑合同数量\n" +
      s"SELECT a.order_src,'black_cnt' title,count(distinct a.order2) cnt " +
      s"FROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='2' \n" +
      s"and a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前 \n" +
      s"and a.apply_time_src>a.apply_time1\nand a.label2 = 1\ngroup by a.order_src\n" +
      s"-- 2度_Q标拒绝数量  2  \nunion all \nSELECT a.order_src,'q_refuse_cnt' title,\n" +
      s"count(distinct a.order2) cnt \nFROM fqz_order_data_inc a     --order订单表现  \n" +
      s"where performance2='q_refuse' \nand a.degree_type='2' \n" +
      s"and a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前 \n" +
      s"and a.apply_time_src>a.apply_time1\ngroup by a.order_src \nunion all\n" +
      s"-- 2度_通过合同数量  2  \nSELECT a.order_src,'pass_cnt' title,\n" +
      s"count(distinct a.order2) cnt \nFROM fqz_order_data_inc a     --order订单表现\n" +
      s"where a.type2='pass'  \nand a.degree_type='2' \n" +
      s"and a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前\n" +
      s"and a.apply_time_src>a.apply_time1\ngroup by a.order_src " +
      s"\n--合并二度关联数据 \n" ).registerTempTable("overdue_cnt_1_2_instant")
    hc.sql(s"select order_src,\nsum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt ,           \n" +
      s"sum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt ,   \n" +
      s"sum(case when title= 'black_cnt' then cnt else 0 end ) black_cnt , \n" +
      s"sum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt ,    \n" +
      s"sum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt \nfrom  overdue_cnt_1_2_instant\n" +
      s"group by order_src").registerTempTable("overdue_cnt_2_sum_instant")

    //02_0f_overdue_data.sql
    hc.sql(s"select c.order_src,\n" +
      s"        'overdue0' title\n       ,count(distinct c.order2) cnt \n       from \n" +
      s"   fqz_order_data_inc c \n where c.type2='pass'       --通过 \n" +
      s"   and c.current_due_day2<=0  --当前\n   and c.degree_type='2' \n" +
      s"   and c.apply_time_src>c.apply_time1  \n   and c.apply_time_src>c.apply_time2 \n" +
      s"   group by c.order_src\nunion all\nselect c.order_src,\n        'overdue3' title\n " +
      s"      ,count(distinct c.order2) cnt \n       from \n   fqz_order_data_inc c \n" +
      s" where c.type2='pass'       --通过 \n   and c.current_due_day2>3 --当前 \n" +
      s"   and c.degree_type='2' and c.apply_time_src>c.apply_time1 \n" +
      s"   and c.apply_time_src>c.apply_time2    \n  group by c.order_src\nunion all\n" +
      s"select c.order_src,\n        'overdue30' title\n       ,count(distinct c.order2) cnt \n" +
      s"       from \n   fqz_order_data_inc c \n where c.type2='pass'       --通过 \n" +
      s"   and c.current_due_day2>30  --当前\n   and c.degree_type='2' and c.apply_time_src>c.apply_time1 \n" +
      s"   and c.apply_time_src>c.apply_time2 \n  group by c.order_src \nunion all\nselect c.order_src,\n" +
      s"        'overdue0_ls' title\n       ,count(distinct c.order2) cnt \n       from \n" +
      s"   fqz_order_data_inc c \n where c.type2='pass'       --通过 \n " +
      s"  and c.history_due_day2<=0 --历史\n   and c.degree_type='2' and c.apply_time_src>c.apply_time1 \n" +
      s"   and c.apply_time_src>c.apply_time2    \n  group by c.order_src\nunion all\nselect c.order_src,\n" +
      s"        'overdue3_ls' title\n       ,count(distinct c.order2) cnt \n       from \n" +
      s"   fqz_order_data_inc c \n where c.type2='pass'       --通过 \n " +
      s"  and c.history_due_day2> 3 --历史\n   and c.degree_type='2' and c.apply_time_src>c.apply_time1  \n" +
      s"   and c.apply_time_src>c.apply_time2 \n  group by c.order_src\nunion all\nselect c.order_src,\n" +
      s"        'overdue30_ls' title\n       ,count(distinct c.order2) cnt \n       from \n" +
      s"   fqz_order_data_inc c \n where c.type2='pass'       --通过  \n " +
      s"  and c.history_due_day2> 30 --历史\n   and c.degree_type='2' and c.apply_time_src>c.apply_time1 \n" +
      s"   and c.apply_time_src>c.apply_time2    \n   group by c.order_src\n   \n" +
      s"--合并二度关联 逾期数据\n" ).registerTempTable("overdue_cnt_2_2_tmp_instant")
    hc.sql(s"select order_src,\n" +
      s"sum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           \n" +
      s"sum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   \n" +
      s"sum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  \n" +
      s"sum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, \n" +
      s"sum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,\n" +
      s"sum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls \n" +
      s"from  overdue_cnt_2_2_tmp_instant\n" +
      s"group by order_src").registerTempTable("overdue_cnt_2_2_instant")

    //03_overdue_result_all.sql
    hc.sql(s"select trim(order_src) order_src   \n" +
      s"from fqz_order_data_inc\ngroup by order_src \n--宽表合并\n" ).registerTempTable("order_src_group_instant")
    hc.sql(s"select \na.order_src,\nnvl(b.order_cnt_self,0) as order_cnt_self,\n" +
      s"nvl(b.id_cnt_self,0) as id_cnt_self,\nnvl(b.black_cnt_self,0) as black_cnt_self,\n" +
      s"nvl(b.q_refuse_cnt_self,0) as q_refuse_cnt_self,\nnvl(b.pass_cnt_self,0) as pass_cnt_self,\n" +
      s"nvl(c.overdue0,0) as overdue0_self,\nnvl(c.overdue3,0) as overdue3_self,\n" +
      s"nvl(c.overdue30,0) as overdue30_self,\nnvl(c.overdue0_ls,0) as overdue0_ls_self,\n" +
      s"nvl(c.overdue3_ls,0) as overdue3_ls_self,\nnvl(c.overdue30_ls,0) as overdue30_ls_self,\n" +
      s"nvl(d.order_cnt,0) as order_cnt,\nnvl(d.id_cnt,0) as id_cnt,\n" +
      s"nvl(d.black_cnt,0) as black_cnt,\nnvl(d.q_refuse_cnt,0) as q_refuse_cnt,\n" +
      s"nvl(d.pass_cnt,0) as pass_cnt,\nnvl(e.overdue0,0) as overdue0,\nnvl(e.overdue3,0) as overdue3,\n" +
      s"nvl(e.overdue30,0) as overdue30,\nnvl(e.overdue0_ls,0) as overdue0_ls ,\n" +
      s"nvl(e.overdue3_ls,0) as overdue3_ls,\nnvl(e.overdue30_ls,0) as overdue30_ls, \n" +
      s"nvl(f.order_cnt,0) as order_cnt_2,\nnvl(f.id_cnt,0)       as id_cnt_2,\n" +
      s"nvl(f.black_cnt,0)       as black_cnt_2,\nnvl(f.q_refuse_cnt,0) as q_refuse_cnt_2,\n" +
      s"nvl(f.pass_cnt,0)    as pass_cnt_2,\nnvl(g.overdue0,0)    as overdue0_2,\n" +
      s"nvl(g.overdue3,0)    as overdue3_2,\nnvl(g.overdue30,0)   as overdue30_2,\n" +
      s"nvl(g.overdue0_ls,0) as overdue0_ls_2,\nnvl(g.overdue3_ls,0) as overdue3_ls_2,\n" +
      s"nvl(g.overdue30_ls,0) as overdue30_ls_2\nfrom order_src_group_instant a   --256441\n" +
      s"left join overdue_cnt_self_sum_instant b on a.order_src = b.order_src  " +
      s"-- 一度关联自身（订单数量、ID数量、黑合同数量、Q标拒绝数量 ）\n" +
      s"left join overdue_cnt_2_self_instant c on a.order_src = c.order_src    " +
      s"-- 合并一度关联 逾期数据(关联自身)\n" +
      s"left join overdue_cnt_1_sum_instant d on a.order_src = d.order_src     " +
      s"-- 一度关联排除自身（订单数量、ID数量、黑合同数量、Q标拒绝数量 ）\n" +
      s"left join overdue_cnt_2_instant  e on a.order_src = e.order_src        " +
      s"-- 一度关联 逾期数据(排除自身)\nleft join overdue_cnt_2_sum_instant f on a.order_src = f.order_src" +
      s"     -- 二度关联数据 （订单数量、ID数量、黑合同数量、Q标拒绝数量 ）\n" +
      s"left join overdue_cnt_2_2_instant g on a.order_src = g.order_src      " +
      s"-- 二度关联 逾期数据\n--数据处理，过滤全0值，是否过滤\n" ).registerTempTable("overdue_result_all_instant")

    //保留变量值为0的情况，便于反馈统计
    hc.sql(s"select * from overdue_result_all_instant\nwhere !(\n  order_cnt_self         = 0 and \n" +
      s"  id_cnt_self           = 0 and \n  black_cnt_self       = 0 and\n  q_refuse_cnt_self     = 0 " +
      s"and \n  pass_cnt_self         = 0 and \n  overdue0_self         = 0 and \n  " +
      s"overdue3_self         = 0 and \n  overdue30_self         = 0 and \n  " +
      s"overdue0_ls_self      = 0 and \n  overdue3_ls_self      = 0 and \n " +
      s" overdue30_ls_self     = 0 and \n  order_cnt           = 0 and \n  " +
      s"id_cnt               = 0 and \n  black_cnt            = 0 and\n  q_refuse_cnt         = 0 and \n" +
      s"  pass_cnt           = 0 and \n  overdue0           = 0 and \n  overdue3           = 0 and \n" +
      s"  overdue30           = 0 and \n  overdue0_ls           = 0 and \n " +
      s" overdue3_ls           = 0 and \n  overdue30_ls         = 0 and \n  " +
      s"order_cnt_2           = 0 and \n  id_cnt_2           = 0 and\n  black_cnt_2            = 0 and \n" +
      s"  q_refuse_cnt_2         = 0 and \n  pass_cnt_2           = 0 and \n " +
      s" overdue0_2           = 0 and \n  overdue3_2           = 0 and \n  overdue30_2           = 0 and \n" +
      s"  overdue0_ls_2         = 0 and \n  overdue3_ls_2         = 0 and \n " +
      s" overdue30_ls_2         = 0\n)").registerTempTable("overdue_result_all_instant")

    //04_overdue_result_all.sql
    hc.sql(s"concat(a.c1,'|',a.c3) as ljmx,\n1 as depth \nfrom fqz_order_data_inc a\n" +
      s"where a.degree_type = '1' \nand a.apply_time_src>a.apply_time1 \n--一度关联进件为黑\n" +
      s"and a.label1 = 1 \nunion all \nselect \na.order_src,\n" +
      s"concat(a.c1,'|',a.c3,'|',a.c5,'|',a.c7) as ljmx,\n2 as depth \n" +
      s"from fqz_order_data_inc a\nwhere a.degree_type = '2' \nand a.apply_time_src>a.apply_time1 \n" +
      s"and a.apply_time_src>a.apply_time2    \n--二度关联进件为黑\nand a.label2 = 1\n" +
      s"--===========聚合关联边\n" ).registerTempTable("order_src_bian_tmp_instant")
    hc.sql(s"select c.order_src," +
      s"concat_ws(',',collect_set(ljmx)) as ljmx           \n" +
      s"from  order_src_bian_tmp_instant  c\ngroup by c.order_src\n\n" +
      s"--边字段 关联到结果表 \n" ).registerTempTable("order_src_bian_instant")
    hc.sql(s"select \n--c.label,\n" +
      s"tab.c5 as apply_time,\na.*,b.ljmx\nfrom overdue_result_all_instant a\n" +
      s"left join order_src_bian_instant b on a.order_src=b.order_src\njoin \n" +
      s"(select c0,c5 from one_degree_data c \nwhere year = ${year} " +
      s"and month = ${month} and day = ${day}\ngroup by c0,c5) tab " +
      s"on a.order_src = tab.c0").registerTempTable("overdue_result_all_new_instant")

    //05_overdue_result_all_new_woe.sql
    hc.sql(s"select order_src,max(depth) as depth from order_src_bian_tmp_instant \n" +
      s"group by order_src\n\n--统计每个订单边权重 , 每个边woe依赖全量统计\n" ).registerTempTable("fqz_edge_depth_instant")
    hc.sql(s"select \na.order_src,\nsum(b.woe) as edge_woe_sum,\nmax(woe) as edge_woe_max,\n" +
      s"min(woe) as edge_woe_min\nfrom \nfqz_edge_data_total_instant a \n" +
      s"join fqz_edge_woe b on a.edge = b.edge   --每个边woe值依赖全局统计\n" +
      s"group by a.order_src\n\n--合并最总结果\n" ).registerTempTable("fqz_order_edge_woe_instant")
    hc.sql(s"select a.*,\nb.edge_woe_sum,\nb.edge_woe_max,\nb.edge_woe_min,\n" +
      s"c.depth\nfrom overdue_result_all_new_instant a \n" +
      s"left join fqz_order_edge_woe_instant b on a.order_src = b.order_src\n" +
      s"left join fqz_edge_depth_instant c on a.order_src = c.order_src").registerTempTable("overdue_result_all_new_woe_instant")
  }

  //备份当前已处理数据，用于增量计算
  def incrementCalculateBackup(): Unit ={
    hc.sql("drop table graphx_tansported_ordernos_history")
    hc.sql("create table graphx_tansported_ordernos_history as \n" +
      "select * from graphx_tansported_ordernos_current")
  }
}
