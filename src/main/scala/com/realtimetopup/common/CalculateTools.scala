package com.realtimetopup.common

import java.text.SimpleDateFormat

object CalculateTools {
  def getDate(requestId: String, endTime:String) = {

    if (endTime.equals("null")){
      endTime
    }
    //取出请求id前面的所有时间
    val startTime = requestId.substring(0,17)
    //解析取出来的时间,转换成时间格式
    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    //相减得出相差时间
    format.parse(endTime).getTime - format.parse(startTime).getTime

  }


}
