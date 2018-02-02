package lakala.models.antiFraud

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.neo4j.test.rule.SuppressOutput.System
import tachyon.util.CommonUtils

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Exception

/**
  * Created by Administrator on 2017/7/14
  * 反欺诈风险评分
  */
object AntiFraudScore /*extends Logging*/{
  val sparkConf = new SparkConf().setAppName("AntiFraudScore")
  val sc = new SparkContext(sparkConf)
  val hc = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    if(args.length!=10){
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

    //logWarning("start calculate ....")
    //println("start calculate ....")
    //批量打分
    try{
      predictScore(database,table,path,host,user,password, port,mysqlDB,mysqlTable,mysqlTableNew)
    }catch {
      case ex: Exception => /*logError(ex.getMessage)*/ println(ex.getMessage)
      //保存该批次失败的order_id,apply_time
        hc.sql(s"select order_src,apply_time from $database.$table").write
          .mode(SaveMode.Append).saveAsTable("lkl_card_score.fqz_score_fail_record")
    }
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
         password:String, port:String,mysqlDB:String,mysqlTable:String,mysqlTableNew:String): Unit ={
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

}
