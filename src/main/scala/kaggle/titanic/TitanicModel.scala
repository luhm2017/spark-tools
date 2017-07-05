package kaggle.titanic;

import breeze.linalg.max
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by lhm on 2017/6/4.
  */
object TitanicModel {

  val sparkConf = new SparkConf().setAppName("kaggle.Titanic")
  val sc = new SparkContext(sparkConf)
  val hc = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    sparkConf.setAppName("kaggle.titanic.LogisticRegressionForTitanic")

    if(args.length!=1){
      println("请输入参数：文件加载路径filePath")
      System.exit(0)
    }
    //文件路径
    //val filePath = args(0)
    //load csv
    //val rawData = loadCsvData(filePath)
    //--==============================================================================================
    val filePath = "file:///home/hadoop/exportdata/train_titanic.csv"
    val rawData = hc.read.format("com.databricks.spark.csv").option("header", "true").load(filePath)
    rawData.registerTempTable("training_data")
    //数据勘探
    rawData.printSchema()
    //最大值、最小值、中位数、平均值、总数
    rawData.describe("Pclass", "Sex", "Age", "SibSp", "Parch", "Fare", "Embarked").show()
    //数据分布勘探
    //rawData.groupBy("Survived").agg("Age" -> "max", "salary" -> "avg")
    //rawData.agg("Fare" -> "max", "Age" -> "min", "Age" -> "avg", "Age" -> "count", "Age" -> "sum")

    //数据特征处理
    //船舱等级
    val pclassData = rawData.select("PassengerId","Pclass")
    //对pclass onehot编码，返回 RDD[(Double,Vector)]
    val pclass = oneHotEncoder(pclassData)
    //
    // 数据预处理
    val trainData = rawData.map{ d =>
      //特征变量处理
      val arr = new ArrayBuffer[Double]()
      //PassengerId
      val passengerId = d(0).toString
      //获取label数据，Survived
      val label = d(1).toString.toDouble
      //Sex处理
      val sex = d(4) match {
        case "male" => 0.0
        case "female" => 1.0
      }
      //Age处理，补齐缺失值方式：1）、平均值替代；2）、根据姓名称呼等其他特征推断年龄
      //归一化处理
      /*val age = d(5) match {
        //case null => (ageMean - ageMin) / ageDiff
        case null => 30.0
        //case _ => (d(5).toString().toDouble - ageMin) / ageDiff
      }*/
      //船票费用，空值处理
      val fare = d(9) match {
        //case null => (fareMean - fareMin) / fareDiff
        case null => 32.0
        //case _ => (d(9).toString().toDouble - fareMin) / fareDiff
      }
      (sex, fare)
    }




    trainData.take(10)
    //features process
    //val stat1 = Statistics.colStats()
  }

  //====================================================================================
  /**
    * load data
    * @param filePath "file:///home/hadoop/exportdata/train_titanic.csv"
    * @param database lkl_card_score
    * @param table    kaggle_titanic_train_data
    * */
  def loadCsvData(filePath:String ,database:String,table:String): Unit ={
    sparkConf.setAppName("kaggle.titanic.loadCsvData")
    //val filePath = "file:///home/hadoop/exportdata/train_titanic.csv"
    val csvDF = hc.read.format("com.databricks.spark.csv").option("header", "true").load(filePath)
    csvDF.write.mode(SaveMode.Overwrite).saveAsTable(s"$database.$table")
  }

  /**
    * 加载csv格式数据
    * */
  def loadCsvData2(filePath:String):DataFrame={
    val csvDF = hc.read.format("com.databricks.spark.csv").option("header", "true").load(filePath)
    return csvDF
  }

  /**
    * onehot编码，处理标称型变量
    * */
  def oneHotEncoder(df:DataFrame):RDD[(Double,Vector)] ={
    //索引化dataframe，即统计获取特征的属性类别
    val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(df)
    //获取属性类别对应的统计值
    val indexed = indexer.transform(df)
    //获取属性类别对应的稀疏矩阵
    val encoder = new OneHotEncoder().setInputCol("categoryIndex").setOutputCol("categoryVec")
    val encoded = encoder.transform(indexed)
    val data = encoded.map { x =>
      {
        //稀疏矩阵转换成稠密矩阵
        val featureVector = Vectors.dense(x.getAs[org.apache.spark.mllib.linalg.SparseVector]("categoryVec").toArray)
        val passengerId = x.getString(0).toDouble
        (passengerId,featureVector)
      }
    }
    return data
  }

  //features process
  def processData(): DataFrame ={


    return null
  }

}
