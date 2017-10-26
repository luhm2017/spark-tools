package kaggle.creditCard

import org.apache.spark.mllib.linalg.{DenseVector, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by pattrick on 2017/9/6.
  * 数据集来源，http://archive.ics.uci.edu/ml/datasets/default+of+credit+card+clients
  * 台湾某商业银行信用卡实际数据
  * 分别选择逻辑回归和CART决策树算法
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

  //特征变量处理
  def variableProcess(): Unit ={
    //特征数据

    val df = hc.sql(s"select label,limit_bal,sex,education,marriage,age,pay_0,pay_2,	" +
      s"pay_3,pay_4,pay_5,pay_6,bill_amt1,bill_amt2,bill_amt3,bill_amt4,bill_amt5,bill_amt6," +
      s"pay_amt1,pay_amt2,pay_amt3,pay_amt4,pay_amt5,pay_amt6 " +
      s"from lkl_card_score.credit_card_client_variable")
    //StringIndexer
    val indexer1 = new StringIndexer().setInputCol("pay_0").setOutputCol("pay_0Index")
    val indexed1 = indexer1.fit(df).transform(df)

    val indexer2 = new StringIndexer().setInputCol("pay_2").setOutputCol("pay_2Index")
    val indexed2 = indexer2.fit(df).transform(indexed1)

    val indexer3 = new StringIndexer().setInputCol("pay_3").setOutputCol("pay_3Index")
    val indexed3 = indexer3.fit(df).transform(indexed2)

    val indexer4 = new StringIndexer().setInputCol("pay_4").setOutputCol("pay_4Index")
    val indexed4 = indexer4.fit(df).transform(indexed3)

    val indexer5 = new StringIndexer().setInputCol("pay_5").setOutputCol("pay_5Index")
    val indexed5 = indexer5.fit(df).transform(indexed4)

    val indexer6 = new StringIndexer().setInputCol("pay_6").setOutputCol("pay_6Index")
    val indexed6 = indexer6.fit(df).transform(indexed5)


    indexed6.write.mode(SaveMode.Overwrite).saveAsTable("lkl_card_score.credit_card_client_variable20171025")

    //VectorIndexer 多变量
    //val data = hc.read.format("libsvm").load("/user/luhuamin/fqz_data/sample_libsvm_data.txt")
    /*val data = hc.sql(s"select label,pay_0,pay_2,pay_3,pay_4 from lkl_card_score.credit_card_client_variable").map{
      row =>
        val arr = new ArrayBuffer[Double]()
        for(i <- 1 until row.size){
          if(row.isNullAt(i)){
            arr += 0.0
          }else if(row.get(i).isInstanceOf[Double])
            arr += row.getDouble(i)
        }
        Row(Vectors.dense(arr.toArray))
    }

    val schema = StructType(
      Seq(StructField("features",StringType,true)
      )
    )
    val features = hc.createDataFrame(data,schema)
    val indexer = new VectorIndexer().setInputCol("features").setOutputCol("indexed").setMaxCategories(10)
    val indexerModel = indexer.fit(features)
    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} categorical features: " +
      categoricalFeatures.mkString(", "))
    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(features)
    indexedData.show()*/

  }



  /**
    * 最优化分箱方法
    * 连续型变量最优分箱处理
    * 一般思路：等宽、等频、k-means或者使用决策树根据熵、信息值、基尼方差、皮尔森卡方
    * */
  def binContVar(): Unit ={
    //input待决策分类的源数据
    val trainingData = hc.sql(s"select label,age from lkl_card_score.credit_card_client_variable20171025").map{
      row =>
        val arr = new ArrayBuffer[Double]()
        arr += row.getDouble(1)
        //arr += row.getDouble(2)
        LabeledPoint(row.getDouble(0), Vectors.dense(arr.toArray))
    }
    //使用决策数据划分叶子节点
    //参数设定
    val numClasses = 2
    //离散型变量预处理

    //离散变量增加类别标志
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "entropy"
    val maxDepth = 3
    val maxBins = 100
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,impurity, maxDepth, maxBins)
    //val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity,maxDepth, maxBins)
    println(model.toDebugString)

  }

  /**
    * 训练模型并评估
    * 评估指标：
    * 1、分类准确率
    * 2、AUC
    * 3、召回率recall
    * 4、精准度precision
    * */
  def trainModel(): Unit ={
    //提取数据集 RDD[LabeledPoint]
    val data = hc.sql(s"select * from lkl_card_score.credit_card_client_variable20171025_bin").map{
      row =>
        val arr = new ArrayBuffer[Double]()
        for(i <- 1 until row.size){
          if(row.isNullAt(i)){
            arr += 0.0
          }else if(row.get(i).isInstanceOf[Double])
            arr += row.getDouble(i)
          else if(row.get(i).isInstanceOf[Integer])
            arr += row.getInt(i).toDouble
        }
        LabeledPoint(row.getDouble(0), Vectors.dense(arr.toArray))
    }

    // Split data into training (60%) and test (40%)
    val Array(trainingData, testData) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    // 逻辑回归是迭代算法，所以缓存训练数据的RDD
    trainingData.cache()
    //使用SGD算法运行逻辑回归
    val numIterations = 1000
    val stepSize = 1
    val miniBatchFraction = 1.0
    val model = LogisticRegressionWithSGD.train(trainingData, numIterations, stepSize, miniBatchFraction)

    //训练模型
    //val model = lrLearn.run(trainingData)

    //decisionTree =========================================================================
    /*val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "variance"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity,
      maxDepth, maxBins)*/


    //random forest =================================================================
    /*val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 6 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "variance"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)*/

    //gbdt ==========================================================================
    /*val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.setNumIterations(30) // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.setMaxDepth(6)
    boostingStrategy.treeStrategy.setMinInstancesPerNode(50)

    // Empty categoricalFeaturesInfo indicates all features are continuous.
    //boostingStrategy.treeStrategy.setCategoricalFeaturesInfo( scala.collection.mutable.Map[Int, Int]())
    //boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)*//*val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.setNumIterations(30) // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.setMaxDepth(6)
    boostingStrategy.treeStrategy.setMinInstancesPerNode(50)

    // Empty categoricalFeaturesInfo indicates all features are continuous.
    //boostingStrategy.treeStrategy.setCategoricalFeaturesInfo( scala.collection.mutable.Map[Int, Int]())
    //boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)*/


    // Clear the prediction threshold so the model will return probabilities
    //默认 threshold = 0.5
    //model.clearThreshold
    //model.setThreshold(0.5)
    // Compute raw scores on the testData set
    val predictionAndLabels = testData.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
   // predictionAndLabels.take(100)
    //使用BinaryClassificationMetrics评估模型
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    // AUC
    val auROC = metrics.areaUnderROC
    println(auROC)

  }

}
