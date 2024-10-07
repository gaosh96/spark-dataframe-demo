package com.yxdata.df.select

import com.yxdata.df.utils.SparkSessionUtils

/**
 *
 * na 处理空值
 *
 * @author gaosh
 * @version 1.0
 * @since 2024/10/6
 */
object NaApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionUtils.init()

    // 指定字段和 null 的填充值
    spark.table("yx_ods.ods_na_df")
      .na.fill(Map(
        "mobile_no" -> "-1",
        "address" -> "-1"
      )).show


  }

}
