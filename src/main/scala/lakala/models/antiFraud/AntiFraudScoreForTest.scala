package lakala.models.antiFraud


import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import java.util.Properties

/**
  * Created by Administrator on 2017/7/14
  * 反欺诈风险评分
  */
object AntiFraudScoreForTest {
  val sparkConf = new SparkConf().setAppName("AntiFraudScore")
  val sc = new SparkContext(sparkConf)
  val hc = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    if(args.length!=5){
      println("请输入参数：database、table、path、numTrees、maxDepth")
      System.exit(0)
    }

    //分别传入库名、表名、对比效果路径
    val database = "lkl_card_score"
    val table = ""
    val modelType = args(2)
    val numTrees = args(3)
    val maxDepth = args(4)

    //提取数据集 RDD[LabeledPoint]
    val data = hc.sql(s"select * from lkl_card_score.overdue_result_all_new_woe_20170629 where label <> 2").map {
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
    }
  }

  //训练模型
  def trainRFModel(data: RDD[LabeledPoint], numTrees:Int, maxDepth:Int,modelType:String): Unit = {
    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 30 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "variance"
    val maxDepth = 6
    //最大分箱数，必须大于最大的离散特征值数
    val maxBins = 50
    val model = RandomForest.trainRegressor(data, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    //save the model
    model.save(sc, s"hdfs://ns1/user/luhuamin/fqz0720/model/rf")
  }

  //训练模型
  def trainGBDTModel(data: RDD[LabeledPoint], numTrees:Int, maxDepth:Int,modelType:String): Unit ={
    //train a gbdt model
    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.setNumIterations(30) // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.setMaxDepth(6)
    boostingStrategy.treeStrategy.setMinInstancesPerNode(50)

    // Empty categoricalFeaturesInfo indicates all features are continuous.
    //boostingStrategy.treeStrategy.setCategoricalFeaturesInfo( scala.collection.mutable.Map[Int, Int]())
    //boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
    val model = GradientBoostedTrees.train(data, boostingStrategy)
    //save the model
    model.save(sc, s"hdfs://ns1/user/luhuamin/fqz0720/model/gbdt")
    //model evaluate
    /*val predictionAndLabels = data.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    //使用BinaryClassificationMetrics评估模型
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    metrics.areaUnderROC()*/
  }

  //预测打分,并保存到mysql
  def predictScore(modelType:String,database:String,table:String): Unit ={
    //实时数据
    val dataInstance = hc.sql(s"select * from lkl_card_score.overdue_result_all_new_woe_instant").map {
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
    //加载模型
    if(modelType.equals("RF")){
      val model = RandomForestModel.load(sc,s"hdfs://ns1/user/luhuamin/fqz0720/model/rf")
      //打分
      val preditDataRF = dataInstance.map { point =>
        val prediction = model.predict(point._2)
        (point._1, prediction)
      }
      preditDataRF.collect()
      //preditData保存
      preditDataRF.saveAsTextFile(s"hdfs://ns1/user/luhuamin/fqz0720/predictionAndLabels/rf")
    }
    else if(modelType.equals("GBDT")){
      val model = GradientBoostedTreesModel.load(sc,s"hdfs://ns1/user/luhuamin/fqz0720/model/gbdt")
      //打分
      val preditDataGBDT = dataInstance.map { point =>
        val prediction = model.predict(point._2)
        (point._1, prediction)
      }
      //preditData保存
      //preditDataGBDT.saveAsTextFile(s"hdfs://ns1/user/luhuamin/fqz0720/predictionAndLabels/gbdt")
      //通过StructType直接指定每个字段的schema
      val schema = StructType(
        List(
          StructField("order_id", StringType, true),
          StructField("score", StringType, true)
        )
      )
      //将RDD映射到rowRDD
      val rowRDD = preditDataGBDT.map(row => Row(row._1.toString,row._2.toString))
      val sqlContext = new SQLContext(sc)
      //将schema信息应用到rowRDD上
      val personDataFrame = sqlContext.createDataFrame(rowRDD,schema)
      val host = "10.16.65.31"
      val user = "root"
      val password = "123_lakala"
      val port = "3306"
      val db = "anti_fraud"
      val table = "fqz_result"
      val url = s"jdbc:mysql://$host:$port/$db?user=$user&password=$password&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false"
      personDataFrame.write.mode(SaveMode.Append).jdbc(url,table,new Properties())
    }
  }

  //load to mysql
  def FS2JDBC(): Unit ={
    val host = "10.16.65.31"
    val user = "root"
    val password = "123_lakala"
    val port = "3306"
    val db = "anti_fraud"
    val table = "fqz_result"
    val url = s"jdbc:mysql://$host:$port/$db?user=$user&password=$password&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false"
    //val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    //preditDataGBDT
    val input = sqlContext.read.load("hdfs://ns1/user/luhuamin/fqz0720/predictionAndLabels/gbdt")
    input.write.mode(SaveMode.Append).jdbc(url,table,new Properties())
    //异常处理
  }

  //load to hive
  def FS2Hive(): Unit ={

  }

}
