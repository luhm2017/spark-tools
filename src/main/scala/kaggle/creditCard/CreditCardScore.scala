package kaggle.creditCard

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by pattrick on 2017/9/6.
  */
object CreditCardScore {
  val sparkConf = new SparkConf().setAppName("CreditCardScore")
  val sc = new SparkContext(sparkConf)
  val hc = new HiveContext(sc)

  def main(args: Array[String]): Unit = {

  }

  //加载csv数据
  def loadCsvData(path:String,database:String,table:String): Unit ={
    //多文件
    //val inputPath2 = "file:///data/data2/exportblackorder02_to_now/level2/pool-1-thread-11.csv,file:///data/data2/exportblackorder02_to_now/level2/pool-1-thread-12.csv,file:///data/data2/exportblackorder02_to_now/level2/pool-1-thread-13.csv,file:///data/data2/exportblackorder02_to_now/level2/pool-1-thread-14.csv,file:///data/data2/exportblackorder02_to_now/level2/pool-1-thread-15.csv,file:///data/data2/exportblackorder02_to_now/level2/pool-1-thread-16.csv,file:///data/data2/exportblackorder02_to_now/level2/pool-1-thread-1.csv,file:///data/data2/exportblackorder02_to_now/level2/pool-1-thread-2.csv,file:///data/data2/exportblackorder02_to_now/level2/pool-1-thread-3.csv,file:///data/data2/exportblackorder02_to_now/level2/pool-1-thread-4.csv,file:///data/data2/exportblackorder02_to_now/level2/pool-1-thread-5.csv,file:///data/data2/exportblackorder02_to_now/level2/pool-1-thread-6.csv,file:///data/data2/exportblackorder02_to_now/level2/pool-1-thread-7.csv,file:///data/data2/exportblackorder02_to_now/level2/pool-1-thread-8.csv,file:///data/data2/exportblackorder02_to_now/level2/pool-1-thread-9.csv"
    val inputPath1 = "/user/luhuamin/train-data/default of credit card clients.csv"
    val csvDF = hc.read.format("com.databricks.spark.csv").option("header", "false").load(inputPath1)
    //csvDF.write.mode(SaveMode.Overwrite).saveAsTable("lkl_card_score.fqz_score_order_201703_20170810_data1")
    csvDF.write.mode(SaveMode.Overwrite).saveAsTable("lkl_card_score.credit_card_client")
  }

  //单变量分析处理
  def variableProcess(): Unit ={
    //特征数据
    val variableData = hc.sql(s"select * from lkl_card_score.credit_card_client_variable")
    //variableData.describe()
  }

  //计算单变量的信息值
  def woe_single_x(data:DataFrame): Unit ={
    val variableData = hc.sql(s"select * from lkl_card_score.credit_card_client_variable")
  }

  //最优化分箱方法
  def binContVar(): Unit ={

  }

  /**
    * 训练模型并评估
    * */
  def trainModel(): Unit ={
    //提取数据集 RDD[LabeledPoint]
    val data = hc.sql(s"select * from lkl_card_score.credit_card_client_variable").map{
      row =>
        val arr = new ArrayBuffer[Double]()
        for(i <- 1 until row.size){
          if(row.isNullAt(i)){
            arr += 0.0
          }else if(row.get(i).isInstanceOf[Double])
            arr += row.getDouble(i)
        }
        LabeledPoint(row.getDouble(0), Vectors.dense(arr.toArray))
    }

    // Split data into training (60%) and test (40%)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 11L)
    // 逻辑回归是迭代算法，所以缓存训练数据的RDD
    trainingData.cache()
    //使用SGD算法运行逻辑回归
    val lrModel = new LogisticRegressionWithSGD
    //lrModel.optimizer.setStepSize(0.01)
    //lrModel.optimizer.setMiniBatchFraction(1.0)
    //lrModel.optimizer.setRegParam(1)
    //训练模型
    val model = lrModel.run(trainingData)
    // Clear the prediction threshold so the model will return probabilities
    //默认 threshold = 0.5
    model.clearThreshold
    // Compute raw scores on the testData set
    val predictionAndLabels = testData.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    predictionAndLabels.take(100)
    //使用BinaryClassificationMetrics评估模型
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    // AUC
    val auROC = metrics.areaUnderROC
    println(auROC)

  }

}
