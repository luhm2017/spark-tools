package lakala.models

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.{DecisionTree, GradientBoostedTrees, RandomForest}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by lhm on 2017/6/6.
  */
object LakalaCardScore {

  val sparkConf = new SparkConf().setAppName("LakalaCardScore")
  val sc = new SparkContext(sparkConf)
  val hc = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    sparkConf.setAppName("LakalaCardScore")
    if(args.length!=3){
      println("请输入参数：trainingData对应的库名、表名、模型运行时间")
      System.exit(0)
    }

    //分别传入库名、表名、对比效果路径
    val database = args(0)
    val table = args(1)
    val savePath = args(2)

    //提取数据集 RDD[LabeledPoint]
    val data = hc.sql(s"select * from $database.$table").map{
      row =>
        val arr = new ArrayBuffer[Double]()
        //剔除处理label、contact字段
        for(i <- 2 until row.size){
          if(row.isNullAt(i)){
            arr += 0.0
          }else if(row.get(i).isInstanceOf[Double])
            arr += row.getDouble(i)
          else if(row.get(i).isInstanceOf[Long])
            arr += row.getLong(i).toDouble
        }
        //label、contact数据单独处理
        (row.getDouble(0),row.getString(1),LabeledPoint(row.getDouble(0), Vectors.dense(arr.toArray)))
    }

    //--===============================================================================
    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 6 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "variance"
    val maxDepth = 4
    val maxBins = 32

    //全量数据训练模型
    val trainData = data.map(row => row._3)
    val model = RandomForest.trainRegressor(trainData, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // 全量数据预测打分
    val predictionAndLabels = data.map { point =>
      val prediction = model.predict(point._3.features)
      (point._1,point._2, prediction)
    }
    //保存
    predictionAndLabels.saveAsTextFile(s"hdfs://ns1/tmp/$savePath/predictionAndLabels")

    //--======================================================================
    //gbdt94
    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.setNumIterations(9) // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.setMaxDepth(4)
    val gdbt94_model = GradientBoostedTrees.train(trainData, boostingStrategy)

    // 全量数据预测打分
    val gbdt94_predictionAndLabels = data.map { point =>
      val prediction = gdbt94_model.predict(point._3.features)
      (point._1,point._2, prediction)
    }
    //保存
    gbdt94_predictionAndLabels.saveAsTextFile(s"hdfs://ns1/tmp/$savePath/predictionAndLabels")

    //--==========================================================================
    //--dt
    val dt_categoricalFeaturesInfo = Map[Int, Int]()
    val dt_impurity = "variance"
    val dt_maxDepth = 5
    val dt_maxBins = 32

    val dt_model = DecisionTree.trainRegressor(trainData, dt_categoricalFeaturesInfo, dt_impurity, dt_maxDepth, dt_maxBins)

    // 全量数据预测打分
    val dt_predictionAndLabels = data.map { point =>
      val prediction = dt_model.predict(point._3.features)
      (point._1,point._2, prediction)
    }
    //保存
    dt_predictionAndLabels.saveAsTextFile(s"hdfs://ns1/tmp/$savePath/predictionAndLabels")

  }
}
