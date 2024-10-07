package com.yxdata.df.udf

import com.yxdata.df.utils.SparkSessionUtils
import org.apache.spark.sql.functions.udf

/**
 * @author gaosh
 * @version 1.0
 * @since 2024/10/7
 */
object UdfApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionUtils.init()

    // 一般的注册方式，这种方式不能传入 Column 对象，一般在 sql 中使用
    spark.udf.register("lenUdf", lenUdf _)

    spark.sql(
      """
        |select year, month
        |from yx_ods.ods_bj_pm_df
        |where lenUdf(month) > 1
        |
        |""".stripMargin).show

    // 这种方式可以传入 Column 对象
    import spark.implicits._
    val len = udf(lenUdf _)
    spark.table("yx_ods.ods_bj_pm_df")
      .where(len($"month") > 1)
      .show


  }

  def lenUdf(str: String): Int = str.length

}
