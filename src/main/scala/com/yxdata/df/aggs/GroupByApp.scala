package com.yxdata.df.aggs

import com.yxdata.df.utils.SparkSessionUtils

/**
 * @author gaosh
 * @version 1.0
 * @since 2024/10/6
 */
object GroupByApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionUtils.init()

    import org.apache.spark.sql.functions._

    // 2015 年 PM_Dongsi 和 PM_Dongsihuan 的最大，最小，平均值
    spark.table("yx_ods.ods_bj_pm_df")
      .where("year = 2015")
      .groupBy("year")
      .agg(
        max("PM_Dongsihuan").as("maxPM_Dongsihuan"),
        min("PM_Dongsihuan").as("minPM_Dongsihuan"),
        avg("PM_Dongsihuan").as("avgPM_Dongsihuan"),
        max("PM_Dongsi").as("maxPM_Dongsi"),
        min("PM_Dongsi").as("minPM_Dongsi"),
        avg("PM_Dongsi").as("avgPM_Dongsi"),
        // lit 代表一个常量，例如 count(1) 里面的 1 就要用 lit(1)
        count(lit(1)).as("cnt")
      ).show

    spark.sql(
      """
        |select
        | year
        | , max(PM_Dongsihuan) as maxPM_Dongsihuan
        | , min(PM_Dongsihuan) as minPM_Dongsihuan
        | , avg(PM_Dongsihuan) as avgPM_Dongsihuan
        | , max(PM_Dongsi) as maxPM_Dongsi
        | , min(PM_Dongsi) as minPM_Dongsi
        | , avg(PM_Dongsi) as avgPM_Dongsi
        | , count(1) as cnt
        | from yx_ods.ods_bj_pm_df
        | where year = 2015
        | group by year
        |""".stripMargin).show


  }

}
