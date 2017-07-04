package lakala.sparktools.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 自定义spark UDAF函数，数组类型求平均
  */
class UDAFArrAvg(len:Int) extends UserDefinedAggregateFunction {

  //定义输入
  override def inputSchema: StructType = StructType(StructField("arr",ArrayType(DoubleType))::Nil)

  //定义单个partition中的操作
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1l
    buffer(1) = (buffer.getAs[Seq[Double]](1) zip input.getAs[Seq[Double]](0)).map(x=>x._1+x._2)
  }

  override def bufferSchema: StructType = StructType(StructField("long",LongType)::StructField("arr2",ArrayType(DoubleType))::Nil)

  //定义不同partition之间的操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = (buffer1.getAs[Seq[Double]](1) zip buffer2.getAs[Seq[Double]](1)).map(x=>x._1+x._2)
  }

  //对聚合运算中间结果的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0l
    buffer(1) = new Array[Double](len)
  }

  override def deterministic: Boolean = true

  override def evaluate(buffer: Row): Any = {
    val num = buffer.getAs[Long](0)
    buffer.getAs[Seq[Double]](1).map(_/num)
  }

  override def dataType: DataType = ArrayType(DoubleType)
}
