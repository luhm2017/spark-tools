package lakala.models.antiFraud

import java.util.Properties

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Exception

/**
  * Created by Administrator on 2017/7/14
  * 反欺诈风险评分
  */
object AntiFraudScore {
  val sparkConf = new SparkConf().setAppName("AntiFraudScore")
  val sc = new SparkContext(sparkConf)
  val hc = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    if(args.length!=8){
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

    //val numTrees = args(3)
    //val maxDepth = args(4)

    //提取数据集 RDD[LabeledPoint]
    /*val data = hc.sql(s"select * from $database.$table where label <> 2").map {
      row =>
        val arr = new ArrayBuffer[Double]()
        //剔除label、phone字段
        for (i <- 3 until row.size) {
          if (row.get(i).isInstanceOf[Double])
            arr += row.getDouble(i)
          else if (row.get(i).isInstanceOf[Long])
            arr += row.getLong(i).toDouble
          else
            arr += 0.0
        }
       LabeledPoint(row.getDouble(0), Vectors.dense(arr.toArray))
    }*/
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
         password:String, port:String,mysqlDB:String,mysqlTable:String): Unit ={
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
        (row(1), Vectors.dense(arr.toArray))
    }
    //加载模型，目前只考虑gbdt
    val model = GradientBoostedTreesModel.load(sc,s"hdfs://ns1/user/luhuamin/$path/model/gbdt")
    //打分数据
    val preditDataGBDT = dataInstance.map { point =>
      val prediction = model.predict(point._2)
      (point._1, prediction)
    }
    //rdd转dataFrame
    val rowRDD = preditDataGBDT.map(row => Row(row._1.toString,row._2.toString))
    val schema = StructType(
      List(
        StructField("order_id", StringType, true),
        StructField("score", StringType, true)
      )
    )
    //将RDD映射到rowRDD
    val sqlContext = new SQLContext(sc)
    //将schema信息应用到rowRDD上
    val scoreDataFrame = sqlContext.createDataFrame(rowRDD,schema)
    //分别保存至mysql和hive
    FS2JDBC(model,scoreDataFrame,host,user,password,port,mysqlDB,mysqlTable)
    FS2Hive(scoreDataFrame)
  }

  //load to mysql
  def FS2JDBC(model:GradientBoostedTreesModel,dataInstance:DataFrame,host:String,user:String,password:String,
              port:String,mysqlDB:String,mysqlTable:String): Unit ={

      val url = s"jdbc:mysql://$host:$port/$mysqlDB?user=$user&password=$password&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false"
      dataInstance.write.mode(SaveMode.Append).jdbc(url,mysqlTable,new Properties())
      //考虑异常处理
  }

  //load to hive
  def FS2Hive(dataInstance:DataFrame): Unit ={
    dataInstance.write.mode(SaveMode.Append).saveAsTable("lkl_card_score.fqz_score_result")
  }

}
