package cn.ljj.spark.sparksql.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * 统计spark作业
  */
object TopNStatJob {
  /**
    * 按流量统计topN课程
    * @param spark
    * @param accessDF
    * @param day
    */
  def videoAccessTopNStat(spark:SparkSession,accessDF:DataFrame,day:String): Unit ={
    /**
      * 使用dataframe的方式统计
      *
      */
    import spark.implicits._
    val videoAccessTopDF=accessDF.filter($"day"===day && $"classType"==="video")
      .groupBy("day","classId").agg(count("classId").as("times")).orderBy($"times".desc)
    //videoAccessTopDF.show(false)
    /**
      * 使用sql的方式进行统计
      */
    /*accessDF.createOrReplaceTempView("access_logs")
    val videoAccessTopDF1=spark.sql("select day,classId,count(1) as times from access_logs " +
      "where day="+day+" and classType='video' " +
      "group by day,classId order by times desc")
    videoAccessTopDF1.show(false)*/
    /**
      * 将统计结果写入到mysql
      *
      */
    try {
      videoAccessTopDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVedioAccessStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val classId = info.getAs[Long]("classId")
          val times = info.getAs[Long]("times")
          list.append(DayVedioAccessStat(day, classId, times))
        })
        //将数据插入数据库
        StatDAO.insertDayVedioAccessTopN(list)
      })
    } catch {
      case e:Exception =>e.printStackTrace()
    }
  }

  /**
    * 按地市统计topN课程
    * @param spark
    * @param accessDF
    * @param day
    */
  def cityAccessTopNStat(spark:SparkSession,accessDF:DataFrame,day:String): Unit ={
    import spark.implicits._
    val cityAccessTopDF=accessDF.filter($"day"===day && $"classType"==="video")
      .groupBy("day","classId","city").agg(count("classId").as("times"))
    // window函数在spark sql的使用
    val top3DF=cityAccessTopDF.select(
      cityAccessTopDF("day"),
      cityAccessTopDF("city"),
      cityAccessTopDF("classId"),
      cityAccessTopDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopDF("city"))
        .orderBy(cityAccessTopDF("times")))
        .as("times_rank")
    ).filter("times_rank<=3")
    /**
      * 将统计结果写入到mysql
      *
      */
    try {
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val classId = info.getAs[Long]("classId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val times_rank = info.getAs[Int]("times_rank")
          list.append(DayCityVideoAccessStat(day, classId,city, times,times_rank))
        })
        //将数据插入数据库
        StatDAO.insertDayCityVedioAccessTopN(list)
      })
    } catch {
      case e:Exception =>e.printStackTrace()
    }
  }

  /**
    * 按流量统计topN课程
    *
    * @param spark
    * @param accessDF
    * @param day
    * @return
    */
  def videoTrafficsTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {

    import spark.implicits._

    val videoAccessTopDF = accessDF.filter($"day" === day && $"classType" === "video")
      .groupBy("day", "classId").agg(sum("traffic").as("traffics")).orderBy($"traffics".desc)

    //    videoAccessTopDF.show(false)

    /**
      * 将统计结果写入mysql
      */
    try {
      videoAccessTopDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayTrafficVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val classId = info.getAs[Long]("classId")
          val traffics = info.getAs[Long]("traffics")

          list.append(DayTrafficVideoAccessStat(day, classId, traffics))
        })

        // 将数据插入数据库
        StatDAO.insertDayVedioTrafficsAccessTopN(list)

      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\DevelopmentEnvironment\\hadoop-common-2.2.0-bin-master")
    val spark = SparkSession.builder()
      .appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()
    val accessDF=spark.read.format("parquet").load("D:\\ljj\\TestData\\log_clean")
    //accessDF.printSchema()
    //accessDF.show(30, false)
    val day = "20161110"
    //写入数据前先删除
    //StatDAO.deleteData(day)
    // 最受欢迎的topN课程
    //videoAccessTopNStat(spark, accessDF, day)
    //按地市统计topN课程
    cityAccessTopNStat(spark, accessDF, day)
    //按流量统计topN课程
    //videoTrafficsTopNStat(spark, accessDF, day)
    spark.stop()
  }

}
