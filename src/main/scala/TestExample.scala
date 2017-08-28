

/**
  * Created by Administrator on 2017/2/16.
  */
object TestExample {
  def main(args: Array[String]): Unit = {
    var myArray : Array[String] = new Array[String](10);
    for(i <- 0 until myArray.length){
      myArray(i) = "value is: " + i;
    }
  }
}
