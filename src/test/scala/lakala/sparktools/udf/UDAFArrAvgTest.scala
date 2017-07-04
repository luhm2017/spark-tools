package lakala.sparktools.udf

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 16-6-15.
  */
object UDAFArrAvgTest {

  def testFunction(): Unit ={
    val conf = new SparkConf().setAppName("UDAF").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val nums = List(Array(1.0,4.5), Array(2.0,2.4), Array(3.0,1.8))
    val numsRDD = sc.parallelize(nums, 1)

    val numsRowRDD = numsRDD.map { x => Row(x) }

    val structType = StructType(Array(StructField("num", ArrayType(DoubleType))))

    val numsDF = sqlContext.createDataFrame(numsRowRDD, structType)

    numsDF.registerTempTable("numtest")
//    sqlContext.sql("select name,avg(num) from numtest group by name").collect().foreach { x => println(x) }

    sqlContext.udf.register("avgarr2", new UDAFArrAvg(2))
    sqlContext.sql("select avgarr2(num) from numtest").collect().foreach { x => println(x.mkString("(",",",")")) }
  }

  def main(args: Array[String]) {
    testFunction()
  }
}
