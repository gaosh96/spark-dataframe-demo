package com.yxdata.df.select

import com.yxdata.df.utils.SparkSessionUtils

/**
 * @author gaosh
 * @version 1.0
 * @since 2024/10/6
 */
object ExplodeApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionUtils.init()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    spark.table("yx_ods.ods_province_category_df")
      // 对于 explode， select 和 withColumn 两种方式都可以
      .select($"province", explode(split($"categories", ",")).as("category"))
      //.withColumn("category", explode(split($"categories", ",")))
      .select("province", "category")
      .show

    spark.table("yx_ods.ods_students_score_df")
      // select 中每次只能写一个 posexplode
      // 无法在 withColumn 中使用 posexplode
      .select($"class", $"scores", posexplode(split($"students", ",")).as(Seq("sn_pos", "student_name")))
      .select($"class", $"sn_pos", $"student_name", posexplode(split($"scores", ",")).as(Seq("sc_pos", "student_score")))
      .where("sn_pos = sc_pos")
      .select("class", "student_name", "student_score")
      .show
  }


}
