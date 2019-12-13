package com.realtimetopup.common

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

/**
  * Redis数据库池
  */
object Jpools {

  def  getJedis={
    //连接池
    //设置连接池配置
    val config = new JedisPoolConfig
    //设置最大连接数
    config.setMaxTotal(20)
    //设置最大空闲连接数
    config.setMaxIdle(10)
    //设置连接超时时间
    config.setMaxWaitMillis(10)

    //创建jedis-------单机操作
    //如果有密码要验证
    //连接 配置 池
    val pool = new JedisPool(config,"hadoop06", 6379)
    //获取连接
    val jedis: Jedis = pool.getResource
    jedis

  }

}