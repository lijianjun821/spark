package cn.ljj.spark.sparksql.log

import com.ggstar.util.ip.IpHelper

/**
  * ip解析 工具类
  */
object IpUtils {
  def getCity(ip:String)={
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    println(getCity("218.75.35.226"))
  }

}
