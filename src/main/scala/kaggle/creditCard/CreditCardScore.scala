package kaggle.creditCard

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext

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

  //计算单变量的信息值
  def woe_single_x(data:DataFrame): Unit ={

  }

  //最优化分箱方法
  def binContVar(): Unit ={

  }

  //评分卡制作，单变量分数值处理

}
