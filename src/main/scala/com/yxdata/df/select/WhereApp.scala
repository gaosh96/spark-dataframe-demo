package com.yxdata.df.select

import com.yxdata.df.utils.SparkSessionUtils

/**
 *
 * where
 *
 * @author gaosh
 * @version 1.0
 * @since 2024/10/6
 */
object WhereApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionUtils.init()

    spark.table("yx_ods.ods_bj_pm_df")
      .where("year > 2013")
      .show

    import spark.implicits._
    spark.table("yx_ods.ods_bj_pm_df")
      .where($"year" > 2013)
      .show

  }

}
