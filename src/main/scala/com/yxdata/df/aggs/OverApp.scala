package com.yxdata.df.aggs

import com.yxdata.df.utils.SparkSessionUtils
import org.apache.spark.sql.expressions.Window

/**
 *
 * over 开窗函数
 * 以连续登录问题为例
 *
 * @author gaosh
 * @version 1.0
 * @since 2024/10/6
 */
object OverApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionUtils.init()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    spark.table("yx_ods.ods_user_login_df")
      .withColumn("dt", from_unixtime(unix_timestamp($"date", "yyyyMMdd"), "yyyy-MM-dd"))
      .withColumn("tmp_dt", date_sub($"dt", row_number().over(Window.partitionBy($"user_id").orderBy($"dt"))))
      .groupBy($"user_id", $"tmp_dt")
      .agg(
        count(lit(1)).as("days")
      )
      // 没有 having 方法，直接使用 where 筛选聚合后的结果
      .where($"days" >= 3)
      // 只 select 需要展示的列
      .select("user_id", "days")
      .show

    spark.sql(
      """
        |with t as (
        |    select from_unixtime(unix_timestamp(`date`, 'yyyyMMdd'), 'yyyy-MM-dd') as dt, user_id from yx_ods.ods_user_login_df
        |), t2 as (
        |    select
        |        user_id
        |        , date_sub(dt, row_number() over(partition by user_id order by dt)) as tmp_dt
        |    from t
        |)
        |
        |select user_id, count(1) as days
        |from t2
        |group by user_id, tmp_dt
        |having count(1) >= 3
        |
        |""".stripMargin).show




  }

}
