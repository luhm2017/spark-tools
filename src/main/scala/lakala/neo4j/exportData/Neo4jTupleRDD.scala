package lakala.neo4j.exportData

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.neo4j.driver.v1.Driver

import scala.collection.JavaConverters._

class Neo4jTupleRDD(@transient sc: SparkContext, val query: String, val parameters: Seq[(String, AnyRef)])
  extends RDD[Seq[(String, AnyRef)]](sc, Nil) {

  private val config = Neo4jConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[Seq[(String, AnyRef)]] = {
    val driver: Driver = config.driver()
    val session = driver.session()

    val result = session.run(query, parameters.toMap.asJava)

    result.asScala.map( (record) => {
      val res = record.asMap().asScala.toSeq
      if (!result.hasNext) {
        session.close()
        driver.close()
      }
      res
    })
  }

  override protected def getPartitions: Array[Partition] = Array(new Neo4jPartition())
}

object Neo4jTupleRDD {
  def apply(sc: SparkContext, query: String, parameters: Seq[(String,AnyRef)] = Seq.empty) = new Neo4jTupleRDD(sc, query, parameters)
}


