package com.realtimetopup.common


import java.sql.Connection

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

/**
  * 工具
  */

object ApiUtils {


  /**
    * 过滤解析原始数据,返回用得到的数据,并且保存到内存中
    *
    * @param baseRdd 包含原始数据的rdd
    * @return (data, hour,List[Double](1, SuccedResult._1, SuccedResult._2, SuccedResult._3), provinceCode,minute)
    *         (日期,小时,List(1,1,充值金额,时间间隔),省份,分钟)
    */
  def Api_BaseDate(baseRdd: RDD[ConsumerRecord[String, String]]) = {

    //过滤数据
    val cleanRDD: RDD[JSONObject] = baseRdd.map(rdd => {
      // 解析源数据成json对象
      JSON.parseObject(rdd.value())
    }).filter(jsonObt => {
      jsonObt.getString("serviceName")
        .equalsIgnoreCase("reChargeNotifyReq")
    })

    val baseDataRDD = cleanRDD.map(jsonObject => {
      //获取业务结果---0000成功
      val bussinessRst: String = jsonObject.getString("bussinessRst")
      //获取充值金额
      val chargefee: Double = if (bussinessRst.equals("0000")) jsonObject.getDouble("chargefee") else 0.0
      // 获取省份
      val provinceCode: String = jsonObject.getString("provinceCode")
      // 获取日期
      val requestId: String = jsonObject.getString("requestId")
      // 小时
      val data: String = requestId.substring(0, 8)
      // 小时
      val hour: String = requestId.substring(8, 10)
      //分钟
      val minute: String = requestId.substring(10, 12)
      //获取结束时间---20170412030929385

      val endReqTime: String = jsonObject.getString("receiveNotifyTime")

      println("-----------------------------")
      println("业务结果:" + bussinessRst + "\n充值金额:" + chargefee + "\n省份:" + provinceCode + "\n获取日期:" + requestId + "\n结束时间:" + endReqTime)
      //求出请求到结束的时间
      val time = CalculateTools.getDate(requestId, endReqTime)
      // 返回结果(1,充值金额,充值时间),只返回成功的
      val SuccedResult: (Int, Double, Long) = if (bussinessRst.equals("0000")) (1, chargefee, time) else (0, 0, 0)


      (data, hour, List[Double](1, SuccedResult._1, SuccedResult._2, SuccedResult._3), provinceCode, minute)


    }).cache()
    baseDataRDD
  }

  /**
    * 统计全网的
    * 充值订单量
    * 充值金额
    * 充值成功数
    * 充值总时长
    *
    * @param baseDataRDD (日期,小时,List(1,1/0,充值金额,时间间隔),省份,分钟)
    *
    */
  def Api_general_total(baseDataRDD: RDD[(String, String, List[Double], String, String)]): Unit = {
    baseDataRDD.map(
      //取出日期和douboe的list
      tp => (tp._1, tp._3))
      .reduceByKey((list1, list2) => {
        //拉链,并且把拉链后的list中的第一和第二个元素累加返回数值,也就是计算总量和成功量
        list1.zip(list2).map(tp => tp._1 + tp._2)
      }).foreachPartition(partition => {
      //保存到redis中
      val redis = Jpools.getJedis
      partition.foreach(tp => {
        //总量
        redis.hincrBy("A-" + tp._1, "total", tp._2(0).toLong)
        //成功量
        redis.hincrBy("A-" + tp._1, "success", tp._2(1).toLong)
        //总金额
        redis.hincrBy("A-" + tp._1, "money", tp._2(2).toLong)
        //总时长
        redis.hincrBy("A-" + tp._1, "time", tp._2(3).toLong)
      })
      redis.close()
    })
  }

  /**
    * 实时充值业务办理趋势,主要统计全网每分钟的订单量
    */
  def Order_Num_per_Min(baseDataRDD: RDD[(String, String, List[Double], String, String)]) = {
    baseDataRDD.map(line => {
      ((line._1, line._2, line._5), line._3)
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(tp => {
        tp._1 + tp._2
      })
    }).map(result => (result._1, result._2(0))).foreachPartition(result => {
      //保存到redis
      val jedis: Jedis = Jpools.getJedis
      result.foreach(tp => {
        //每分钟的总浏览量
        jedis.hincrBy("A-" + tp._1._1 + tp._1._2 + tp._1._3, "Order_Num_per_Min", tp._2.toLong)
      })
    })


  }


  /**
    * 全国各省充值业务失败分布
    */
  //(日期,小时,List(1,1/0,充值金额,时间间隔),省份,分钟)
  def Province_failed(baseDataRDD: RDD[(String, String, List[Double], String, String)], city: Array[String]): Unit = {
    //对城市进行切分
    baseDataRDD.map(
      rdd => {
        //声明接受变量
        var pronvince: String = null
        var failedNum: String = null
        for (i <- 0 until city.length) {
          val numAndCity: Array[String] = city(i).split(" ")
          if (rdd._4.equals(numAndCity(0))) {
            pronvince = numAndCity(1)
            failedNum = rdd._3(1).toString
          }
        }
        (pronvince,failedNum)
        //取出省份和是否失败

      }).foreachPartition(result => {
      //创建mysql连接
      val mySqlConn: Connection = MysqlUtils.ConnMysql
      //封装语句
      val sql = mySqlConn.prepareStatement("insert into realTimeToUp (province,failed) values (?,?)")
      result.foreach(
        x => {
          //说明参数
          sql.setString(1, x._1.toString)
          sql.setString(2, x._2.toString)
          //执行语句,返回的是成功条数
          val i: Int = sql.executeUpdate()
          //显示是否写入成功
          if (i > 0) println("写入成功") else println("写入失败")
        }
      )
      sql.close()
      mySqlConn.close()
    })


  }

}
