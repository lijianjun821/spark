package cn.ljj.spark.sparksql.log

/**
  * 每天课程访问次数实体类
  * @param day
  * @param classId
  * @param times
  */
case class DayVedioAccessStat(day: String, classId: Long, times: Long)
