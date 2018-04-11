import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by steven on 18-4-11 下午3:09
  * shijia0620@126.com
  *
  * fake出两个月的数据
  */
object makeData {
  def main(args: Array[String]): Unit = {
    val inputFile = "/home/zhiyedan/wisetone/headData.log"
    /*args(0)*/
    val day = 59
    /*args(1).toInt*/
    val outputFile = "/home/zhiyedan/wisetone/make-data/"
    /*args(2)*/
    val conf = new SparkConf().setAppName("make-data").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val originRdd = sc.textFile(inputFile).persist()
    val fileNames = genNames()
    for (i <- 1 to day) {
      val newData = originRdd.map(line => {
        val arr = line.split("\t")
        val newTime = arr(1).toLong + i * 60 * 60 * 24
        line.replace(arr(1), newTime.toString)
      })
        .coalesce(1)
        .saveAsTextFile(outputFile + fileNames(i-1))
    }

  }

  def genNames(): Array[String] = {
    val array: Array[String] = new Array[String](59)
    val prefix = "netLog2016_ZhongXinTong_20180"
    val postfix = "_merge.ok"
    for (i <- 0 to 58) {
      if (i < 28) {
        val day = i + 1
        if (day < 10)
          array(i) = prefix + "20" + day.toString + postfix
        else
          array(i) = prefix + "2" + day.toString + postfix
      } else {
        val day = i - 28 + 1
        if (day < 10)
          array(i) = prefix + "30" + day.toString + postfix
        else
          array(i) = prefix + "3" + day.toString + postfix
      }
    }
    return array
  }
}
