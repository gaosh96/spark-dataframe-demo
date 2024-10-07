package com.yxdata.df.select

import com.yxdata.df.utils.SparkSessionUtils

/**
 *
 * withColumn 新增列
 *
 * @author gaosh
 * @version 1.0
 * @since 2024/10/6
 */
object WithColumnApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionUtils.init()

    import spark.implicits._
    spark.table("yx_ods.ods_bj_pm_df")
      .select("No", "year")
      .withColumn("year", $"year" + 100)
      .show

  }

}
