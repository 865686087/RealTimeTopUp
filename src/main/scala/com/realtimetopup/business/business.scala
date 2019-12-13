package com.realtimetopup.business

import com.realtimetopup.common.{ApiUtils, Jpools}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
object business {
  def main(args: Array[String]): Unit = {
    //关闭日志
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    //获取streamingcontext
    // 初始化Spark配置信息
    val sparkConf = new SparkConf().
      setMaster("local[*]").
      setAppName("StreamWordCount").
      // 开启rddKryo序列化
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      // rdd压缩
      set("spark.rdd.compress", "true").
      // batchSize = partitionNum * 分区数量 * 采样时间
      set("spark.streaming.kafka.maxRatePerPartition", "100").
      //优雅的停止
      set("spark.streaming.stopGracefullyOnShutdown", "true")

    //streamingcontext
    val streamingContext = new StreamingContext(sparkConf, Durations.seconds(3))
    // todo 连接kafka消费数据
    //基本的变量
    // 节点信息
    val BOOTSTRAP_SERVER = "hadoop06:9092,hadoop06:9093,hadoop06:9094"
    // 消费者组
    val GROUP_ID = "g0"
    // 设置要监听的topic,可以设置多个
    val topics = Array("realTimeTopUp")

    //配置kafka参数
    val kafkaParams = Map[String, Object](
      //Kafka服务监听端口
      "bootstrap.servers" -> BOOTSTRAP_SERVER,
      //指定kafka输出key的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
      "key.deserializer" -> classOf[StringDeserializer],
      //指定kafka输出value的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
      "value.deserializer" -> classOf[StringDeserializer],
      //消费者组id，随意指定
      "group.id" -> GROUP_ID,
      //指定从latest(最新,其他版本的是largest这里不行)还是earliest(最早)处开始读取数据
      "auto.offset.reset" -> "latest", // latest
      //设置false,手动维护
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
//    //消费kafka的数据
//    val srcDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
//      //sparkstreaming
//      streamingContext,
//      //本地策略
//      LocationStrategies.PreferBrokers,
//      //订阅策略
//      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    // todo 手动维护偏移量
    val fromOffset = JedisOffset(GROUP_ID,Jpools.getJedis)
    // 创建来自kafka的数据流
    var stream:InputDStream[ConsumerRecord[String, String]] = null
    // 判断是不是第一次读取数据  如果不是第一次读取数据 offset大于0
    if(fromOffset.size > 0){
      stream = KafkaUtils.createDirectStream(
        streamingContext,
        //本地策略
        LocationStrategies.PreferConsistent,
        //订阅策略,这里使用了来自redis的offset
        ConsumerStrategies.Subscribe[String, String](topics,kafkaParams,fromOffset)
      )
    }else{
      // 如果是第一次消费数据 从0开始读取
      stream = KafkaUtils.createDirectStream[String,String](
        streamingContext,
        //本地策略
        LocationStrategies.PreferConsistent,
        //订阅策略
        ConsumerStrategies.Subscribe[String, String](topics,kafkaParams)
      )
      println("第一次消费数据")
    }

    //todo 广播数据

    val cityRDD: RDD[String] = streamingContext.sparkContext.textFile("file:///D:\\大数据\\lu上课笔记\\第三阶段\\三阶段项目\\实时充值\\code\\RealTimeTopUp\\input\\city.txt")
    val value: Array[String] = cityRDD.collect()
    val cityBro = streamingContext.sparkContext.broadcast(value)

    //todo 业务处理
    stream.foreachRDD(baseRdd => {
      // 为了后续更新Offset做准备
      val offsetRanges = baseRdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // 更新偏移量
      for (o <- offsetRanges) {
        // 将Offset更新到Redis
        Jpools.getJedis.hset(GROUP_ID, o.topic + "-" + o.partition, o.untilOffset.toString)
      }
      //过滤解析进来的数据,返回有用的rdd
      val baseDataRDD = ApiUtils.Api_BaseDate(baseRdd)
      // todo 统计全网的充值订单量, 充值金额, 充值成功数、充值总时长
      ApiUtils.Api_general_total(baseDataRDD)

      // todo 实时充值业务办理趋势,主要统计全网每分钟的订单量
      ApiUtils.Order_Num_per_Min(baseDataRDD)

      //todo 全国各省充值业务失败分布
      ApiUtils.Province_failed(baseDataRDD,cityBro.value)

    }
    )




    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
