package lakala.neo4j.exportData

import lakala.graphx.util.Config

/**
  * Created by Administrator on 2017/5/31 0031.
  */
trait DataGenerator {
  def generateUsers(config: Config): Unit
}
