import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by steven on 18-4-3 上午12:00
  * shijia0620@126.com
  */
object LogTimeSeries {
  def main(args: Array[String]): Unit = {
//    val inFile = "/home/zhiyedan/wisetone/netLog2016_ZhongXinTong_20180310_merge.ok"
//    val outFile = "/home/zhiyedan/wisetone/resultDir"
//    val minute = 1
    val inFile = args(0)
    val outFile = args(1)
    val minute:Int = args(2).toInt

    val now:Long = new Date().getTime / 1000
    val startTime:Long = 1514736000

    val conf = new SparkConf().setAppName("log").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputRdd = sc.textFile(inFile)
      .filter(x => x.contains("783233"))
      .map(x => {
        val array = x.split("\t")
        if (array.length < 20)
          (("null",now),0)
        else {
          val time:Long = array(1).toLong / 60 / minute
          ((array(19), time * 60 * minute), 1)
        }
      })
      //过滤时间不符合/mac不符合
      .filter(x => {
        if (x._1._1.contains("-") && x._1._2 < now && x._1._2 >= startTime)
          true
        else
          false
      })
      .reduceByKey(_ + _)
      .sortByKey()
      // 去掉括号
      .map(line => {
      line._1._1 + "\t" + line._1._2 + "\t" + line._2
    })
      .repartition(1) //合成一个文件
      .saveAsTextFile(outFile)
  }
}
