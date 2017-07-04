package kaggle

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
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
  }

  /**
    * load data
    * @param filePath "file:///home/hadoop/exportdata/train_titanic.csv"
    * @param database lkl_card_score
    * @param table    kaggle_titanic_train_data
    * */
  def loadCsvData(filePath:String ,database:String,table:String): Unit ={
    sparkConf.setAppName("kaggle.titanic.loadCsvData")
    val csvDF = hc.read.format("com.databricks.spark.csv").option("header", "true").load(filePath)
    csvDF.write.mode(SaveMode.Overwrite).saveAsTable(s"$database.$table")
  }

  //features process
  def processData(database:String,table:String): Unit ={
    sparkConf.setAppName("kaggle.titanic.processData")
    //read raw data => RDD[LabeledPoint]
    val rawData = hc.sql(s"select * from $database.$table")
    val data = rawData.map{
      row =>
          val arr = new ArrayBuffer[Double]()
          for(i <- 2 until row.size){

          }
          LabeledPoint(row.getDouble(1), Vectors.dense(arr.toArray))
    }

  }

}
