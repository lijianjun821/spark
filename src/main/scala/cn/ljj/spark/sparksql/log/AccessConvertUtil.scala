package cn.ljj.spark.sparksql.log

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 日志数据转换（输入==>输出）工具类
  */
object AccessConvertUtil {

  //定义输出字段
  val struct = StructType(
    Array(
      StructField("url",StringType),
      StructField("classType",StringType),
      StructField("classId",LongType),
      StructField("traffic",LongType),
      StructField("ip",StringType),
      StructField("city",StringType),
      StructField("time",StringType),
      StructField("day",StringType)
    )
  )

  /**
    * 根据输入每一行信息转换成输出格式
    * @param log
    */
  def parseLog(log:String)={
    try {
      //日志格式：2016-11-10 00:01:02	http://www.imooc.com/code/2053	2954	211.162.33.31
      val splits = log.split("\t")
      val url=splits(1)
      val traffic = splits(2).toLong
      val ip=splits(3)
      val domain = "http://www.imooc.com/"
      val len = url.indexOf(domain)+domain.length
      val className=url.substring(len)
      val classTypeId=className.split("/")
      var classType=""
      var classId=0L
      if(classTypeId.length>1){
        classType=classTypeId(0)
        if(classId.isInstanceOf[Long]){
          classId=classTypeId(1).toLong
        }else{
          classId=0L
        }
      }
      val city = IpUtils.getCity(ip)
      val time=splits(0)
      val day=time.substring(0,10).replace("-","")
      // row要和struct对应上
      Row(url, classType, classId, traffic, ip, city, time, day)
    } catch {
      case e:Exception =>Row("","",0L,0L,"","","","")
    }
  }

  def main(args: Array[String]): Unit = {
    print(parseLog("2016-11-10 00:01:02\t-\t94\t112.10.136.45"))
  }
}
