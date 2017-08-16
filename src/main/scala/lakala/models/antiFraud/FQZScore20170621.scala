package lakala.models.antiFraud

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/6/3.
  * nohup ./spark-submit --master spark://datacenter17:7077,datacenter18:7077 --conf spark.locality.wait=1 --conf spark.driver.memory=1g  --conf spark.executor.cores=4 --total-executor-cores 8 --num-executors 2 --executor-memory 4g --class lakala.models.FQZScore /home/hadoop/spark-tools-1.0.jar lkl_card_score overdue_result_all_new_20170618 20170620fqzscore > ~/log/20170620fqzscore.log  &
  */
object FQZScore20170621 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("FQZScore")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)

    if(args.length!=4){
      System.exit(0)
    }

    val database = args(0)
    val table = args(1)
    val date = args(2)
    val cnt = args(3)

    //随机取label=0数据
    val data0 = hc.sql(s"select * from $database.$table where label =0 distribute by rand() sort by rand() limit $cnt").map{
      row =>
        val arr = new ArrayBuffer[Double]()
        //剔除label、order_id字段
        for(i <- 3 until row.size-3){
              if(row.isNullAt(i)){
                arr += 0.0
              }else if(row.get(i).isInstanceOf[Double])
                arr += row.getDouble(i)
              else if(row.get(i).isInstanceOf[Long])
                arr += row.getLong(i).toDouble
              else arr += 0.0
        }
        LabeledPoint(row.getDouble(0), Vectors.dense(arr.toArray))
    }

    val data1 = hc.sql(s"select * from $database.$table where label = 1 ").map{
      row =>
        val arr = new ArrayBuffer[Double]()
        //剔除label、order_id字段
        for(i <- 3 until row.size-3){
            if(row.isNullAt(i)){
              arr += 0.0
            }else if(row.get(i).isInstanceOf[Double])
              arr += row.getDouble(i)
            else if(row.get(i).isInstanceOf[Long])
              arr += row.getLong(i).toDouble
            else arr += 0.0
        }
        LabeledPoint(row.getDouble(0), Vectors.dense(arr.toArray))
    }

    //合并数据
    val data = data1.union(data0)
    // Split data into training (60%) and test (40%)
    val Array(trainingData, testData) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    // 逻辑回归是迭代算法，所以缓存训练数据的RDD
    trainingData.cache()
    //使用SGD算法运行逻辑回归
    val lrLearner = new LogisticRegressionWithSGD
    //训练模型
    val model = lrLearner.run(trainingData)

    //============================================================================================
    // Clear the prediction threshold so the model will return probabilities
    //默认 threshold = 0.5
    model.clearThreshold
    // Compute raw scores on the testData set
    val predictionAndLabels = testData.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    //使用BinaryClassificationMetrics评估模型
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    // AUC
    val auROC = metrics.areaUnderROC
    sc.makeRDD(Seq("Area under ROC = " + +auROC)).saveAsTextFile(s"hdfs://ns1/user/luhuamin/$date$cnt/auROC")
    //println("Area under ROC = " + auROC)
    //val accuracy = 1.0 * predictionAndLabels.filter(x => x._1 == x._2).count() / testData.count()
  }
}
