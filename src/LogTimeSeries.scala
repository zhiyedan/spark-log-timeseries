import java.util.Date
import org.apache.spark.{SparkConf, SparkContext}

object LogTimeSeries {
  def main(args: Array[String]): Unit = {
    //    val inFile = "/home/steven/awifi/data/netLog2016_ZhongXinTong_20180310_merge.ok"
    //    val outFile = "/home/steven/awifi/data/resultDir"
    //    val minute = 1
    val inFile = args(0)
    val outFile = args(1)
    // 采样间隔
    val minute = args(2).toInt
    // 采样起始时间
    val startTime: Long = args(3).toLong

    val now: Long = new Date().getTime / 1000

    val conf = new SparkConf().setAppName("awifi-log")
    val sc = new SparkContext(conf)

    val inputRdd = sc.textFile(inFile)
      .map(_.split("\t"))
      .filter(array => array.length >= 20 && !array(1).trim.equals("") && array(1).matches("[0-9]*"))
      .map(array => {
        try {
          val time: Long = array(1).toLong / 60 / minute
          ((array(19), time * 60 * minute), 1)
        } catch {
          case ex: Exception => (("null", now), 0)
        }
      })
      //过滤时间不符合/mac不符合
      .filter(x => x._1._1.contains("-") && x._1._2 < now && x._1._2 >= startTime)
      .reduceByKey(_ + _)
      .sortByKey()
      // 去掉括号
      .map(line => {
      line._1._1 + "\t" + line._1._2 + "\t" + line._2
    })
      //      .repartition(1) //合成一个文件
      .saveAsTextFile(outFile)
  }
}
