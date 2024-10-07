package com.yxdata.df.utils

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author gaosh
 * @version 1.0
 * @since 2024/10/6
 */
object SparkSessionUtils {

  def init(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("dataframe test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("create database if not exists yx_ods")

    val pmDataPath = this.getClass.getClassLoader.getResource("BeijingPM20100101_20151231.csv").getPath
    val pmDf = spark.read.option("header", value = true).csv(pmDataPath)
    pmDf.write.mode(SaveMode.Ignore).saveAsTable("yx_ods.ods_bj_pm_df")

    val loginDataPath = this.getClass.getClassLoader.getResource("login.csv").getPath
    val loginDf = spark.read.option("header", value = true).csv(loginDataPath)
    loginDf.write.mode(SaveMode.Ignore).saveAsTable("yx_ods.ods_user_login_df")

    val explodeDataPath = this.getClass.getClassLoader.getResource("explode.csv").getPath
    val explodeDf = spark.read.option("sep", ";").option("header", value = true).csv(explodeDataPath)
    explodeDf.write.mode(SaveMode.Ignore).saveAsTable("yx_ods.ods_province_category_df")

    val posexplodeDataPath = this.getClass.getClassLoader.getResource("posexplode.csv").getPath
    val posexplodeDf = spark.read.option("sep", ";").option("header", value = true).csv(posexplodeDataPath)
    posexplodeDf.write.mode(SaveMode.Ignore).saveAsTable("yx_ods.ods_students_score_df")

    spark
  }


}
