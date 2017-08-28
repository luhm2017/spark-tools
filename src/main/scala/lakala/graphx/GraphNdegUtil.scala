package lakala.graphx

import java.nio.charset.StandardCharsets
import java.security.InvalidParameterException

import com.google.common.hash.Hashing
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by pattrick on 2017/8/22.
  * 用于在图中为指定的节点计算这些节点的N度关系节点
  * 输出这些节点与源节点的路径长度和节点id
  */
object GraphNdegUtil {
  val maxNDegVerticesCount = 10000
  val maxDegree = 1000

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("graphxDemo")
    val sc = new SparkContext(conf)
    //顶点对（cert_no--2,order_id）(email--3,order_id)(mobile--4,order_id)(device--5,order_id)
    val edgeArray = Array(
      (hashId("421083"), hashId("YFQ001")),
      (hashId("luhuamin@lakala.com"), hashId("YFQ001")),
      (hashId("18666956069"), hashId("YFQ001")),
      (hashId("18666956069"), hashId("YFQ002")),
      (hashId("421082"), hashId("YFQ002")),
      //Edge(hashId("devicexxx"), hashId("YFQ002"), EdgeArr("devicexxx","YFQ002","5","1")),
      //Edge(hashId("devicexxx"), hashId("YFQ003"), EdgeArr("devicexxx","YFQ003","5","1")),
      (hashId("lhm@lakala.com"), hashId("YFQ003")),
      (hashId("lhm@lakala.com"), hashId("YFQ004")),
      (hashId("134666"), hashId("YFQ004")),
      (hashId("134666"), hashId("YFQ005")))

    val choosedVertexArray = Array(hashId("YFQ001"))
    val edges: RDD[(VertexId,VertexId)] = sc.parallelize(edgeArray)
    val choosedVertex: RDD[VertexId] = sc.parallelize(choosedVertexArray)
    aggNdegreedVertices(edges,choosedVertex,2)
    // Build the initial Graph
    //val graph = Graph.fromEdges(edges,"")
    //print graph
    //graph.triplets.collect().foreach(println(_))


  }

  /**
    * 计算N度关联节点
    * */
  def aggNdegreedVertices[ED: ClassTag](edges: RDD[(VertexId, VertexId)],
            choosedVertex: RDD[VertexId], degree: Int): VertexRDD[Map[Int, Set[VertexId]]] = {
    val simpleGraph = Graph.fromEdgeTuples(edges, 0, Option(PartitionStrategy.EdgePartition2D), StorageLevel.MEMORY_AND_DISK_SER, StorageLevel.MEMORY_AND_DISK_SER)
    aggNdegreedVertices(simpleGraph, choosedVertex, degree)
  }

  def hashId(str: String) = {
    Hashing.md5().hashString(str, StandardCharsets.UTF_8).asLong()
  }

  private case class Ver[VD: ClassTag](source: VertexId, id: VertexId, degree: Int, attr: VD = null.asInstanceOf[VD])

  private def updateVertexByMsg[VD: ClassTag](vertexId: VertexId, oldAttr: DegVertex[VD], msg: ArrayBuffer[(VertexId, Int)]): DegVertex[VD] = {
    val addOne = msg.map(e => (e._1, e._2 + 1))
    val newMsg = reduceVertexIds(oldAttr.degVertices ++ addOne)
    oldAttr.copy(init = msg.nonEmpty, degVertices = newMsg)
  }

  private def sortResult[VD: ClassTag](degs: DegVertex[VD]): Map[Int, Set[VertexId]] = degs.degVertices.map(e => (e._2, Set(e._1)))/*.reduceByKey(_ ++ _)*/.toMap

  case class DegVertex[VD: ClassTag](var attr: VD, init: Boolean = false, degVertices: ArrayBuffer[(VertexId, Int)])

  case class VertexDegInfo[VD: ClassTag](var attr: VD, init: Boolean = false, degVertices: ArrayBuffer[(VertexId, Int)])

  private def sendMsg[VD: ClassTag](e: EdgeContext[DegVertex[VD], Int, ArrayBuffer[(VertexId, Int)]], sendFilter: (VD, VD) => Boolean): Unit = {
    try {
      val src = e.srcAttr
      val dst = e.dstAttr
      //只有dst是ready状态才接收消息
      if (src.degVertices.size < maxNDegVerticesCount && (src.init || dst.init) && dst.degVertices.size < maxNDegVerticesCount && !isAttrSame(src, dst)) {
        if (sendFilter(src.attr, dst.attr)) {
          e.sendToDst(reduceVertexIds(src.degVertices))
        }
        if (sendFilter(dst.attr, dst.attr)) {
          e.sendToSrc(reduceVertexIds(dst.degVertices))
        }
      }
    } catch {
      case ex: Exception =>
        println(s"==========error found: exception:${ex.getMessage}," +
          s"edgeTriplet:(srcId:${e.srcId},srcAttr:(${e.srcAttr.attr},${e.srcAttr.init},${e.srcAttr.degVertices.size}))," +
          s"dstId:${e.dstId},dstAttr:(${e.dstAttr.attr},${e.dstAttr.init},${e.dstAttr.degVertices.size}),attr:${e.attr}")
        ex.printStackTrace()
        throw ex
    }
  }

  private def reduceVertexIds(ids: ArrayBuffer[(VertexId, Int)]): ArrayBuffer[(VertexId, Int)] = ArrayBuffer() ++= ids/*.reduceByKey(Math.min)*/

  private def isAttrSame[VD: ClassTag](a: DegVertex[VD], b: DegVertex[VD]): Boolean = a.init == b.init && allKeysAreSame(a.degVertices, b.degVertices)

  private def allKeysAreSame(a: ArrayBuffer[(VertexId, Int)], b: ArrayBuffer[(VertexId, Int)]): Boolean = {
    val aKeys = a.map(e => e._1).toSet
    val bKeys = b.map(e => e._1).toSet
    if (aKeys.size != bKeys.size || aKeys.isEmpty) return false

    aKeys.diff(bKeys).isEmpty && bKeys.diff(aKeys).isEmpty
  }

  /**
    * 查找N度关联关系
    * @param graph 关系图
    * @param choosedVertex  制定顶点
    * @param degree 关联度
    * */
  def aggNdegreedVertices[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                      choosedVertex: RDD[VertexId],
                                                      degree: Int,
                                                      sendFilter: (VD, VD) => Boolean = (_: VD, _: VD) => true
                                                     ): VertexRDD[Map[Int, Set[VertexId]]] = {
    if (degree < 1) {
      throw new InvalidParameterException("度参数错误:" + degree)
    }
    //初始化指定顶点
    val initVertex = choosedVertex.map(e => (e, true)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //
    var g: Graph[DegVertex[VD], Int] = graph.outerJoinVertices(graph.degrees)(
      (_, old, deg) => (deg.getOrElse(0), old))
      .subgraph(vpred = (_, a) => a._1 <= maxDegree)
      //去掉大节点
      .outerJoinVertices(initVertex)((id, old, hasReceivedMsg) => {
      DegVertex(old._2, hasReceivedMsg.getOrElse(false), ArrayBuffer((id, 0))) //初始化要发消息的节点
    }).mapEdges(_ => 0).cache() //简化边属性

    choosedVertex.unpersist(blocking = false)

    var i = 0
    var prevG: Graph[DegVertex[VD], Int] = null
    var newVertexRdd: VertexRDD[ArrayBuffer[(VertexId, Int)]] = null
    while (i < degree + 1) {
      prevG = g
      //发第i+1轮消息
      newVertexRdd = prevG.aggregateMessages[ArrayBuffer[(VertexId, Int)]](sendMsg(_, sendFilter), (a, b) => reduceVertexIds(a ++ b)).persist(StorageLevel.MEMORY_AND_DISK_SER)
      g = g.outerJoinVertices(newVertexRdd)((vid, old, msg) => if (msg.isDefined) updateVertexByMsg(vid, old, msg.get) else old.copy(init = false)).cache()
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      newVertexRdd.unpersist(blocking = false)
      i += 1
    }
    newVertexRdd.unpersist(blocking = false)

    val maped = g.vertices.join(initVertex).mapValues(e => sortResult(e._1)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    initVertex.unpersist()
    g.unpersist(blocking = false)
    VertexRDD(maped)
  }

}
