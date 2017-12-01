package lakala.graphx.graphxData

/**
  * Created by linyanshi on 2017/9/1 0001.
  */
case class NewEdgeArr(srcV: String, dstV: String, var srcType: String, dstType: String, init: Boolean = false)

case class NewEdgeArrDT(srcV: String, dstV: String, timeStr: String, var srcType: String, init: Boolean = false) {
  override def toString: String = s"${srcV},${dstV},${timeStr},${srcType},${init}"
}
case class NewEdgeMergeArrDT(srcV: String, dstV: String, srcTimeStr: String, var dstTimeStr: String,var edgeV: String, init: Boolean = false)
