import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by steven on 18-4-11 下午3:09
  * shijia0620@126.com
  */
object makeData {
  def main(args: Array[String]): Unit = {
    val inputFile = "/home/zhiyedan/wisetone/headData.log"/*args(0)*/
    val day = 10 /*args(1).toInt*/
    val outputFile = "/home/zhiyedan/wisetone/make-data/new-data"/*args(2)*/
    val conf = new SparkConf().setAppName("make-data").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val originRdd = sc.textFile(inputFile).persist()
    val i = 0
    for (i <- 1 to day) {
      val newData = originRdd.map(line => {
        val arr = line.split("\t")
        val newTime = arr(1).toLong + i * 60 * 60 * 24
        line.replace(arr(1), newTime.toString)
      })
          .coalesce(1)
        .saveAsTextFile(outputFile+i.toString)
    }

  }
}
