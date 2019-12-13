package mytest

import com.alibaba.fastjson.JSON

object jsonTry {
  def main(args: Array[String]): Unit = {
    val value: String =
      """{"bussinessRst":"0000",
         "channelCode":"0702",
         "chargefee":"1000",
         "clientIp":"101.204.129.105",
         "gateway_id":"CMPAY",
         "interFacRst":"0000",
         "logOutTime":"20170412030031580",
         "orderId":"384663613178752811",
         "payPhoneNo":"",
         "phoneno":"18200222444",
         "provinceCode":"280",
         "rateoperateid":"1514",
         "receiveNotifyTime":"20170412030031554",
         "requestId":"20170412030013393282687799171031",
         "retMsg":"接口调用成功",
         "serverIp":"10.255.254.10",
         "serverPort":"8714",
         "serviceName":"payNotifyReq",
         "shouldfee":"995",
         "srcChannel":"00",
         "sysId":"01"}
        """
    println(JSON.parseObject(value).size()>=21)
  }



}
