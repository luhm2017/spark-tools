package lakala.graphx.main

/**
  * Created by linyanshi on 2017/9/14 0014.
  * updated by luhuamin 20171129
  */

import lakala.graphx.louvain.{HDFSLouvainRunner, VertexState}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}



/**
  * Execute the louvain distributed community detection.
  * Requires an edge file and output directory in hdfs (local files for local mode only)
  * */
object LouvainDGA {

  // specify command line options and their defaults
  case class Config(
                     input:String = "",
                     output: String = "",
                     master:String="local",
                     appName:String="graphX analytic",
                     jars:String="",
                     sparkHome:String="",
                     parallelism:Int = -1,
                     edgedelimiter:String = ",", //分隔符
                     minProgress:Int = 2000,//Number of vertices that must change communites for the algorithm to consider progress. default=2000
                     progressCounter:Int = 1,//Number of times the algorithm can fail to make progress before exiting. default=1
                     ipaddress: Boolean = false,
                     properties:Seq[(String,String)]= Seq.empty[(String,String)] )

  def main(args: Array[String]) {

    if(args.length!=4){
      println("请输入参数：edgeFilePath、outPutDir、minProgress、progressCounter")
      System.exit(0)
    }
    //输入参数
    val edgeFilePath = args(0)
    val outPutDir = args(1)
    val minProgress = args(2).toInt
    val progressCounter = args(3).toInt

    val sparkConf = new SparkConf().setAppName("LouvainDGA")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[VertexState]))
    val sc = new SparkContext(sparkConf)
    //数据源构造
    //1、demo数据测试 2、测试空手道网络数据
    val data = sc.textFile(edgeFilePath)
    val edges = data.map(line => {
      val items = line.split(",")
      //权重默认设置为1
      Edge(items(1).toLong, items(2).toLong, 1.toDouble)
    })
    //构造初始图，顶点默认属性为1
    val graph = Graph.fromEdges(edges, 1)
    val runner = new HDFSLouvainRunner(minProgress, progressCounter, outPutDir)
    runner.run(sc, graph)
    sc.stop()
  }
}

