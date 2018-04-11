import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by steven on 18-4-3 上午12:00
  * shijia0620@126.com
  */
object LogTimeSeries {
  def main(args: Array[String]): Unit = {
    val inFile = "D:\\netLog2016_ZhongXinTong_20180310_merge.ok"
    val outFile = "D:\\netResult"
    val minute = 1
    val conf = new SparkConf().setAppName("awifi-log-1minute").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val inputRdd = sc.textFile(inFile)
        .filter( x => x.contains("783233"))
      .map(x => {
        val array = x.split("\t")
        val time = array(1).toLong/60/minute.toInt
        ((array(19), time*60*minute.toInt), 1)
      })
      .reduceByKey(_ + _)
      .sortByKey()
      // 去掉括号
      .map(line => {
      line._1._1 +"\t" + line._1._2 + "\t" + line._2
    })
      .repartition(1) //合成一个文件
      .saveAsTextFile(outFile)
  }
}
