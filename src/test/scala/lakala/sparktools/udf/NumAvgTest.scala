package lakala.sparktools.udf

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType

/**
  * @author Administrator
  */
object NumAvgTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDAF").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val nums = List(("a",4.5), ("a",2.4), ("b",1.8))
    val numsRDD = sc.parallelize(nums, 1)

    val numsRowRDD = numsRDD.map { x => Row(x._1,x._2) }

    val structType = StructType(Array(StructField("name", StringType),StructField("num", DoubleType)))

    val numsDF = sqlContext.createDataFrame(numsRowRDD, structType)

    numsDF.registerTempTable("numtest")
    sqlContext.sql("select name,avg(num) from numtest group by name").collect().foreach { x => println(x) }

    sqlContext.udf.register("numsAvg", new NumsAvg)
    sqlContext.sql("select name,numsAvg(num) from numtest group by name").collect().foreach { x => println(x) }
  }
}
