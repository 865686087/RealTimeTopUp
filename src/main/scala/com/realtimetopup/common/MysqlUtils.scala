package com.realtimetopup.common

import java.sql.{Connection, DriverManager}

object MysqlUtils {


  def ConnMysql={
    //定义连接参数
    //驱动
    val driver = "com.mysql.jdbc.Driver"
    //连接地址
    val url = "jdbc:mysql://hadoop06:3306/realTimeTopUp"
    //用户
    val userName = "root"
    //密码
    val passWd = "123456"
    //创建连接
    val connection: Connection = DriverManager.getConnection(url, userName, passWd)
    connection

  }




}
