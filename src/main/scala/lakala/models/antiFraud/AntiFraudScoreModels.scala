package lakala.models.antiFraud

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{GradientBoostedTrees}
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import scala.collection.mutable.ArrayBuffer

/**
  * Created by pattrick on 2017/8/12.
  */
object AntiFraudScoreModels {
  val sparkConf = new SparkConf().setAppName("AntiFraudScore")
  val sc = new SparkContext(sparkConf)
  val hc = new HiveContext(sc)

  def main(args: Array[String]): Unit = {

    if(args.length!=2){
      println("请输入参数：database、table以及mysql相关参数")
      System.exit(0)
    }

    val database = args(0)
    val table = args(1)

    /*if(args.length!=10){
      println("请输入参数：database、table以及mysql相关参数")
      System.exit(0)
    }

    //分别传入hive库名、hive表名、模型保存path
    val database = args(0)
    val table = args(1)

    //提取数据集 RDD[LabeledPoint]
    val data = hc.sql(s"select * from $database.$table").map{
      row =>
        val arr = new ArrayBuffer[Double]()
        //剔除label、contact字段
        for(i <- 2 until row.size){
          if(row.isNullAt(i)){
            arr += 0.0
          }else if(row.get(i).isInstanceOf[Double])
            arr += row.getDouble(i)
          else if(row.get(i).isInstanceOf[Long])
            arr += row.getLong(i).toDouble
        }
        LabeledPoint(row.getDouble(0), Vectors.dense(arr.toArray))
    }*/
    trainGBDTModel(database,table)
  }

  //训练模型
  /*def trainGBDTModel(data: RDD[LabeledPoint],path:String): Unit ={
    //提取数据集 RDD[LabeledPoint]
    val data = hc.sql(s"select * from lkl_card_score.").map{
      row =>
        val arr = new ArrayBuffer[Double]()
        //剔除label、contact字段
        for(i <- 2 until row.size){
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

    //train a gbdt model
    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    //组合参数调试
    val subSamplingRate = 0.8
    val maxBins = 32
    val minInstancePerNode = 50
    boostingStrategy.treeStrategy.setMinInstancesPerNode(minInstancePerNode)
    boostingStrategy.treeStrategy.setMaxBins(maxBins) //连续型变量分箱数
    boostingStrategy.treeStrategy.setSubsamplingRate(subSamplingRate)
    for(numTrees <- 5 to 200; maxDepth <- 4 to 20){
        // Split data into training (60%) and test (40%)
        val Array(trainingData, testData) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
        // 逻辑回归是迭代算法，所以缓存训练数据的RDD
        trainingData.cache()
        //============================start time
        val start_time: Long = System.currentTimeMillis()
        boostingStrategy.setNumIterations(numTrees)
        boostingStrategy.treeStrategy.setMaxDepth(maxDepth)
        //train model
        val model = GradientBoostedTrees.train(trainingData, boostingStrategy)
        //evaluation model on test data
        val predictionAndLabels = testData.map { point =>
          val prediction = model.predict(point.features)
          (point.label, prediction)
        }
        //===================================================================
        //使用BinaryClassificationMetrics评估模型
        val metrics = new BinaryClassificationMetrics(predictionAndLabels)
        // Precision by threshold
        val precision = metrics.precisionByThreshold.filter(x => x._1%0.1 ==0)
        //precision avg
        val precisionAvg = precision.map(x => x._2).reduce(_+_)/precision.count
        // Recall by threshold
        val recall = metrics.recallByThreshold.filter(x => x._1%0.1 ==0)
        //recall avg
        val recallAvg = recall.map(x => x._2).reduce(_+_)/recall.count
        //the beta factor in F-Measure computation.
        val f1Score = metrics.fMeasureByThreshold.filter(x => x._1%0.1 ==0)
        // flScore avg
        val flScoreAvg = f1Score.map(x => x._2).reduce(_+_)/f1Score.count
        //合并precision、recall、f1score
        //auc
        val auc = metrics.areaUnderROC()
        //end time
        val costTime = (System.currentTimeMillis()- start_time) / 1000.0
        //打印所有参数，模型参数、模型评估效果
        print("numTrees:" +numTrees+",maxDepth:"+maxDepth+",costTime:"+costTime+",precisionAvg:"+precisionAvg
        +",recallAvg:"+recallAvg+",flScoreAvg:"+flScoreAvg+",AUC:"+auc)
    }
  }*/

  //spark shell 训练模型
  def trainGBDTModel(database:String,table:String): Unit ={
      //读取数据RDD
      val data = hc.sql(s"select * from $database.$table where label <> 2").map{
        row =>
          val arr = new ArrayBuffer[Double]()
          //剔除label、contact字段
          for(i <- 3 until row.size){
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

      //train a gbdt model
      val boostingStrategy = BoostingStrategy.defaultParams("Regression")
      //组合参数调试
      val subSamplingRate = 0.8
      val maxBins = 32
      val minInstancePerNode = 50
      boostingStrategy.treeStrategy.setMinInstancesPerNode(minInstancePerNode)
      boostingStrategy.treeStrategy.setMaxBins(maxBins) //连续型变量分箱数
      boostingStrategy.treeStrategy.setSubsamplingRate(subSamplingRate)
      for(numTrees <- 5 to 150; maxDepth <- 4 to 20){
          // Split data into training (60%) and test (40%)
          val Array(trainingData, testData) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
          // 逻辑回归是迭代算法，所以缓存训练数据的RDD
          trainingData.cache()
          //============================start time
          val start_time: Long = System.currentTimeMillis()
          boostingStrategy.setNumIterations(numTrees)
          boostingStrategy.treeStrategy.setMaxDepth(maxDepth)
          //train model
          val model = GradientBoostedTrees.train(trainingData, boostingStrategy)
          //evaluation model on test data
          val predictionAndLabels = testData.map { point =>
            val prediction = model.predict(point.features)
            (point.label, prediction)
          }
          //===================================================================
          //使用BinaryClassificationMetrics评估模型
          /*val metrics = new BinaryClassificationMetrics(predictionAndLabels)
          // Precision by threshold
          val precision = metrics.precisionByThreshold//.filter(x => x._1%0.1 ==0)
          //precision avg
          val precisionAvg = precision.map(x => x._2).reduce(_+_)/precision.count
          // Recall by threshold
          val recall = metrics.recallByThreshold//.filter(x => x._1%0.1 ==0)
          //recall avg
          val recallAvg = recall.map(x => x._2).reduce(_+_)/recall.count
          //the beta factor in F-Measure computation.
          val f1Score = metrics.fMeasureByThreshold//.filter(x => x._1%0.1 ==0)
          // flScore avg
          val flScoreAvg = f1Score.map(x => x._2).reduce(_+_)/f1Score.count
          //合并precision、recall、f1score
          //auc
          val auc = metrics.areaUnderROC()
          //end time
          val costTime = (System.currentTimeMillis()- start_time) / 1000.0
          //打印所有参数，模型参数、模型评估效果
          println("numTrees:" +numTrees+",maxDepth:"+maxDepth+" ,costTime:"+costTime+" seconds ,precisionAvg:"+precisionAvg
            +",recallAvg:"+recallAvg+",flScoreAvg:"+flScoreAvg+",AUC:"+auc)
          }*/
          //======================================================================
          //使用混淆矩阵BinaryClassificationMetrics评估模型
          val metrics = new BinaryClassificationMetrics(predictionAndLabels)
          // Precision by threshold
          val precision = metrics.precisionByThreshold
          precision.map({case (t, p) =>
            "Threshold: "+t+"Precision:"+p
          })
          // Recall by threshold
          val recall = metrics.recallByThreshold
          recall.map({case (t, r) =>
            "Threshold: "+t+"Recall:"+r
          })
          //the beta factor in F-Measure computation.
          val f1Score = metrics.fMeasureByThreshold
          f1Score.map(x => {"Threshold: "+x._1+"--> F-score:"+x._2+"--> Beta = 1"})

          /**
            * 如果要选择Threshold, 这三个指标中, 自然F1最为合适
            * 求出最大的F1, 对应的threshold就是最佳的threshold
            */
          /*val maxFMeasure = f1Score.select(max("F-Measure")).head().getDouble(0)
          val bestThreshold = f1Score.where($"F-Measure" === maxFMeasure)
            .select("threshold").head().getDouble(0)*/

          // Precision-Recall Curve
          val prc = metrics.pr
          prc.map(x => {"Recall: " + x._1 + "--> Precision: "+x._2 })
          // AUPRC，精度，召回曲线下的面积
          val auPRC = metrics.areaUnderPR
          sc.makeRDD(Seq("Area under precision-recall curve = " +auPRC))
          //roc
          val roc = metrics.roc
          roc.map(x => {"FalsePositiveRate:" + x._1 + "--> Recall: " +x._2})
          // AUC
          val auROC = metrics.areaUnderROC
          sc.makeRDD(Seq("Area under ROC = " + +auROC))
          println("Area under ROC = " + auROC)
          val testMSE = predictionAndLabels.map{ case(v, p) => math.pow((v - p), 2)}.mean()
          sc.makeRDD(Seq("Test Mean Squared Error = " + testMSE))
          sc.makeRDD(Seq("GradientBoostingRegression model: " + model.toDebugString))
      }
  }
}
