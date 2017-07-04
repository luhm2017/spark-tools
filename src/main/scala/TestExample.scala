import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by Administrator on 2017/2/16.
  */
object TestExample {
    def main(args: Array[String]): Unit = {
        /*Vectors.zeros(3*2)
        val vector = Vector(1,2,3,4,5,6,7)
        println(vector.size)
        print(vector.head)*/
        val srcId = 12
        val totalRounds = 3
        val m = Map(srcId -> totalRounds)
        print(m.get(12))


    }
}
