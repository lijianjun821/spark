package cn.ljj.spark.sparksql.log

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * MySQL操作工具类
  */
object MySqlUtils {

  // 获取数据库连接
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://bigdata:3306/bigdata?user=hive&password=hive")
  }

  // 释放数据库连接资源
  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }
  def main(args: Array[String]): Unit = {
    println(getConnection())
  }
}
