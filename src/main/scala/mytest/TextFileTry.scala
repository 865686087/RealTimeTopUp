package mytest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TextFileTry {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("TextFileTry").getOrCreate()
    val strRDD: RDD[String] = session.sparkContext.textFile("file:///D:\\大数据\\lu上课笔记\\第三阶段\\三阶段项目\\实时充值\\code\\RealTimeTopUp\\input\\city.txt")
    strRDD.foreach(x=>{
      val strings: Array[String] = x.split(" ")
      println(strings(0))
    })

  }

}
