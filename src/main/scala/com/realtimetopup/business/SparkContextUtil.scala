package com.realtimetopup.business

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextUtil {
def getsparksession(sparkConf:SparkConf)={
   val sc = new SparkContext(sparkConf)
  sc
  }
}
