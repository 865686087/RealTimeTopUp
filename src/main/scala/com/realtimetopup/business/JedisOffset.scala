package com.realtimetopup.business

import java.util

import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis
/**
  * 获取Offset
  */
object JedisOffset extends Serializable {

  def apply(groupId:String,jedis:Jedis)={
    // 创建Map
    var formOffset = Map[TopicPartition,Long]()
    // 获取连接
    // 查询出每个topic下面的Partition
    val topicPartition: util.Map[String, String] = jedis.hgetAll(groupId)
    import scala.collection.JavaConversions._
    // 将map转换为List
    val topicPartitionOffset = topicPartition.toList
    // 循环输出
    for (topicPL <- topicPartitionOffset){
      // topic-Partition 将数据切分
      val split = topicPL._1.split("-")
      // Offset
      // 将数据存储Map
      formOffset +=(new TopicPartition(split(0),split(1).toInt)->topicPL._2.toLong)
    }
    formOffset
  }

}