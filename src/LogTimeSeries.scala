import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by steven on 18-4-3 上午12:00
  * shijia0620@126.com
  */
object LogTimeSeries {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("log").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val inputRdd = sc.textFile("/home/zhiyedan/wisetone/wiseNetLog")
      .map(x => {
        val array = x.split("\t")
        val time = array(1).toLong/60
        ((array(19), time*60), 1)
      })
      .reduceByKey(_ + _)
      //      .reduceByKey({case (l, r) => l + r})
      .sortByKey()
      // 去掉括号
      .map(line => {
      line._1._1 +"\t" + line._1._2 + "\t" + line._2
    })
      .saveAsTextFile("/home/zhiyedan/wisetone/result.log")
  }
}
