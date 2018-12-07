package cn.ljj.spark.sparksql.log

import org.apache.spark.sql.SparkSession

/**
  * 第一步数据清洗：抽取需要的指定列的数据
  */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\DevelopmentEnvironment\\hadoop-common-2.2.0-bin-master")
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()
    //获取文件
    val access=spark.sparkContext.textFile("D:\\Users\\QDHL\\IdeaProjects\\spark\\10000_access.log")
    val accessFormat = access.map(line=>{
      var splits=line.split(" ")
      val ip=splits(0)
      /**
        * 获取日志中完整的访问时间
        * 并转换日期格式
        */
      val time=splits(3)+" "+splits(4)
      val url=splits(11).replaceAll("\"","")
      val traffic=splits(9)
      DateUtils.parse(time)+"\t"+url+"\t"+traffic+"\t"+ip
    })
    accessFormat.saveAsTextFile("D:\\ljj\\TestData\\spark-output")
    //accessFormat.foreach(println)
    spark.stop()
  }

}
