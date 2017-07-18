package lakala.models.antiFraud

import java.util.Properties

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/7/14
  * 反欺诈风险评分
  */
object AntiFraudScore {
  val sparkConf = new SparkConf().setAppName("AntiFraudScore")
  val sc = new SparkContext(sparkConf)
  val hc = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    if(args.length!=5){
      println("请输入参数：database、table、path、numTrees、maxDepth")
      System.exit(0)
    }

    //分别传入库名、表名、对比效果路径
    val database = args(0)
    val table = args(1)
    val modelType = args(2)
    val numTrees = args(3)
    val maxDepth = args(4)

    //提取数据集 RDD[LabeledPoint]
    val data = hc.sql(s"select * from $database.$table where label <> 2").map {
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
    //val numTrees = 30 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "variance"
    //val maxDepth = 10
    //最大分箱数，必须大于最大的离散特征值数
    val maxBins = 50
    val model = RandomForest.trainRegressor(data, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    //save the model
    model.save(sc, s"hdfs://ns1/user/luhuamin/$modelType/model")
  }

  //训练模型
  def trainGBDTModel(data: RDD[LabeledPoint], numTrees:Int, maxDepth:Int,modelType:String): Unit ={
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
    model.save(sc, s"hdfs://ns1/user/luhuamin/$modelType/model")
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
    val dataInstance = hc.sql(s"select * from $database.$table where label <> 2").map {
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
      val model = RandomForestModel.load(sc,s"hdfs://ns1/user/luhuamin/$modelType/model")
      //打分
      val preditDataRF = dataInstance.map { point =>
        val prediction = model.predict(point._2)
        (point._1, prediction)
      }
      //preditData保存
      //preditDataRF.saveAsTextFile("hdfs://ns1/user/luhuamin/yfq_all/gbdt94/predictionAndLabels")
    }
    else if(modelType.equals("GBDT")){
      val model = GradientBoostedTreesModel.load(sc,s"hdfs://ns1/user/luhuamin/$modelType/model")
      //打分
      val preditDataGBDT = dataInstance.map { point =>
        val prediction = model.predict(point._2)
        (point._1, prediction)
      }
      //preditData保存
      preditDataGBDT.saveAsTextFile(s"hdfs://ns1/user/luhuamin/$modelType/predictionAndLabels")
      //读入mysql
      /*sparkConf.setAppName("spark-tools.JDBC2FS")
      val sc = new SparkContext(sparkConf)
      val sqlContext = new SQLContext(sc)
      val input = sqlContext.read.json(inputPath)
      input.write.mode(SaveMode.Overwrite).jdbc(url,table,new Properties())*/
    }


  }

}
