package lakala.graphx.degreeData;

/**
  * Created by pattrick on 2017/8/22.
  * 使用pregel获取指定顶点的N度邻居子图
  * 将种子顶点属性值设置为1，非种子顶点属性设置为0，
  * 通过Pregel 迭代，每一次将顶点的属性值传递给邻居节点，
  * 每个顶点接收到属性值后与自己的属性值进行累加，
  * 第一次可以传递给一度的邻居顶点，
  * 第二次传递可以传递给二度邻居节点（一度邻居节点会重复接收，但不影响结果），
  * 以此类推，通过N次迭代，第N度的邻居节点能够收到种子节点的值。
  * 最终筛选顶点属性值大于0的顶点构成的子图即为裁剪后的目标子图
  */
object GraphNdegreeSubGraph {
  def main(args: Array[String]) {
    val maxIterations=2
    val conf = new SparkConf().setAppName("GraphNdegreeSubGraph")
    val sc = new SparkContext(conf)

    //初始化顶点，指定顶点的属性设置为1
    val vertexArray: Array[(Long, Int)] = Array(
      (1L, 0),
      (2L, 1),
      (3L, 1),
      (4L, 1),
      (5L, 0),
      (6L, 0),
      (7L, 0),
      (8L, 0),
      (9L, 0),
      (10L, 0),
      (11L, 0),
      (12L, 0),
      (13L, 0)
    )

    //关联边初始属性为1
    val edgeArray: Array[Edge[Int]] = Array(
      Edge(1L, 2L, 1),
      Edge(2L, 3L, 1),
      Edge(3L, 4L, 1),
      Edge(4L, 5L, 1),
      Edge(5L, 6L, 1),
      Edge(6L, 7L, 1),
      Edge(7L, 8L, 1),
      Edge(8L, 9L, 1),
      Edge(9L, 10L, 1),
      Edge(2L, 11L, 1),
      Edge(4L, 12L, 1),
      Edge(12L, 13L, 1)
    )
    //init graph
    val vertexRDD: RDD[(Long, Int)] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    val graph: Graph[Int, Int] = Graph(vertexRDD, edgeRDD)

    //顶点的属性值传递给邻居节点
    def sendMessage(edge: EdgeTriplet[Int, Int]) = {
      Iterator((edge.srcId, edge.dstAttr), (edge.dstId, edge.srcAttr))
    }


    def messageCombiner(a: Int, b: Int): Int = a + b
    val initialMessage = 0

    //每个顶点接收到邻居点属性值后与自己的属性值进行累加
    def vprog(vid: VertexId, vdata: Int, msg: Int): Int = {
      vdata + msg
    }

    val newGraph = Pregel(graph, initialMessage, maxIterations)(vprog, sendMessage, messageCombiner)
    newGraph.triplets.collect().foreach(println(_))

    newGraph.vertices.filter{
      case(vid,vdata)=>{
        vdata>0
      }
    }.sortByKey().collect().foreach(println(_))
  }

}
