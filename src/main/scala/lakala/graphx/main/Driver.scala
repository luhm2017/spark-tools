package lakala.graphx.main

/**
  * Created by Administrator on 2017/5/4 0004.
  */


object Driver extends App {
  override def main(args: Array[String]) = {
    val enD = new ExportNDegreeData()
    enD.main(args)
  }
}
