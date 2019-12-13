package com.realtimetopup.common

import com.typesafe.config.ConfigFactory

/**
  * 主要用于读取配置文件,让其它类调用
  */
object AppParameters {
  /**
    * redis设置
    * 主机名和第一个数据库
    */
  val config = ConfigFactory.load()
val redis_host: String = config.getString("redis.host")
 val redis_index: Int = config.getString("redis.index").toInt
}
