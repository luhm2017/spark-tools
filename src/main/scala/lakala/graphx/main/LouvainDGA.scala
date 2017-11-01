package lakala.graphx.main

/**
  * Created by linyanshi on 2017/9/14 0014.
  */

import lakala.graphx.louvain.{HDFSLouvainRunner, VertexState}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

//totalEdgeWeight: 1.56262281191699E15
//# vertices moved: 61,897,309
//# vertices moved: 13,746,461
//# vertices moved: 5,352,635
//# vertices moved: 130,270
//# vertices moved: 82,426
//# vertices moved: 71,584
//# vertices moved: 71,105
//# vertices moved: 70,030
//# vertices moved: 69,937
//
//Completed in 18 cycles
//
//Starting Louvain level 1
//totalEdgeWeight: 2.237895102976331E15
//# vertices moved: 664,919
//# vertices moved: 191,039
//# vertices moved: 12,426
//# vertices moved: 393
//# vertices moved: 7
//# vertices moved: 0
//
//Completed in 12 cycles
//qValue: 0.9182326588364285
// 总的用户数1232060 总的call_phone yong用户数 101825071
//总的社区 275141 大于两个人的总的社区id 77442  关联黑名单 总的社区 1784

object LouvainDGA {
  def main(args: Array[String]) {
    //传入参数
    //args(0) = "filePath" 构图源数据 /user/guozhijie/explortoutput/${output} 200 1
    //args(1) = "outputFile" result输出路径
    //args(1) = "minProgress"
    //args(2) = "progressCounter"
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("LouvainDGA")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[VertexState]))

   // intputpath  iterator 1 outputpath
    val sc = new SparkContext(conf)
    //数据源构造
    val data = sc.textFile(args(0))
    val edges = data.map(line => {
      val items = line.split(",")
      Edge(items(1).toLong, items(2).toLong, items(3).toDouble)
    })
    //构造初始图
    val graph = Graph.fromEdges(edges, 1)
    val runner = new HDFSLouvainRunner(args(2).toInt, args(3).toInt, args(1))
    runner.run(sc, graph)
    sc.stop()
  }
}

