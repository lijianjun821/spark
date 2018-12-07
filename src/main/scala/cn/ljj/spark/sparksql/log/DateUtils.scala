package cn.ljj.spark.sparksql.log

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期时间解析类
  */
object DateUtils {

  //输入日期时间格式
  val SOURCE_TIME_FROMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
  //目标日期时间格式
  val TARGET_TIME_FROMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    * 获取时间
    * @param time
    * @return
    */
  def parse(time:String)={
    TARGET_TIME_FROMAT.format(new Date(getTime(time)))
  }
  /**
    * 获取输入的时间
    * @param time
    * @return
    */
  def getTime(time:String)={
    try {
      SOURCE_TIME_FROMAT.parse(time.substring(time.indexOf("[") + 1, time.indexOf("]"))).getTime
    } catch {
      case e : Exception =>{
        0L
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(parse("[10/Nov/2016:00:01:02 +0800]"))
  }
}
