package com.yxdata.df.select

import com.yxdata.df.utils.SparkSessionUtils
import org.apache.spark.sql.functions.expr

/**
 *
 * select 和 selectExpr
 *
 * @author gaosh
 * @version 1.0
 * @since 2024/10/6
 */
object SelectApp {

  def main(args: Array[String]): Unit = {

    // 初始化了一张 yx_ods.ods_bj_pm_df 表，可以直接使用
    val spark = SparkSessionUtils.init()

    // 相当于 select * from yx_ods.ods_bj_pm_df limit 20
    spark.table("yx_ods.ods_bj_pm_df").show

    // select 选取部分字段
    spark.table("yx_ods.ods_bj_pm_df")
      .select("No", "year", "month", "day", "hour", "season", "PM_Dongsi")
      .show()

    spark.table("yx_ods.ods_bj_pm_df")
      .selectExpr("No", "year", "month as mon", "round(day) as day")
      .show

    spark.sql(
      """
        |select
        | No
        | , year
        | , month as mon
        | , round(day) as day
        |from yx_ods.ods_bj_pm_df
        |""".stripMargin).show

    // 如果使用 $ 必须要引入隐式转换，会转换为 Column 对象，可以调用 Column 的一些方法，例如 as
    // Column 对象还可以直接使用操作符，例如 + - * / 等，会改变这个字段的值
    // 具体可以参考 Column 的方法
    // cast 支持的类型 string, boolean, byte, short, int, long, float, double, decimal, date, timestamp
    import spark.implicits._
    spark.table("yx_ods.ods_bj_pm_df")
      .select($"No", $"year".cast("string"), $"month".as("mon"), $"day")
      .show()

    spark.table("yx_ods.ods_bj_pm_df")
      .select(expr("No + 100"), expr("round(year) as year"), $"month")
      .show

  }

}
