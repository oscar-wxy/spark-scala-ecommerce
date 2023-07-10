/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 */

import java.util.UUID

import commons.model.{ProductInfo, UserInfo, UserVisitAction}
import commons.utils.{DateUtils, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


/**
  * For mock data generation
  * date：the data of the session
  * age: 0 - 59
  * professionals: professional[0 - 59], we use number here to represent
  * cities: 0 - 9
  * sex: 0 - 1
  * keywords: ("iphone", "ipad", "iwatch", "towels", "vacuum", "shampoo", "steak", "cup", "headphone", "medicine")
  * categoryIds: 0 - 99
  * ProductId: 0 - 99
  */
object MockDataGenerate {

  /**
    * Mock user action data
    *
    * @return
    */
  private def mockUserVisitActionData(): Array[UserVisitAction] = {

    val searchKeywords = Array("iphone", "ipad", "iwatch", "towels", "vacuum", "shampoo", "steak", "cup", "headphone", "medicine")
    // yyyy-MM-dd
    val date = DateUtils.getTodayDate()
    // focus on: search, click, order, pay
    val actions = Array("search", "click", "order", "pay")
    val random = new Random()
    val rows = ArrayBuffer[UserVisitAction]()

    // 100 users, duplicate allowed
    for (i <- 0 to 100) {
      val userid = random.nextInt(100)
      // each user has 10 sessions
      for (j <- 0 to 10) {
        // no change, global
        val sessionid = UUID.randomUUID().toString().replace("-", "")
        // add the hour
        val baseActionTime = date + " " + random.nextInt(23)
        //for each user sessionid, generate 0-100 user action record
        for (k <- 0 to random.nextInt(100)) {
          val pageid = random.nextInt(10)
          // append minute and seconds after yyyy-MM-dd HH
          val actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))
          var searchKeyword: String = null
          var clickCategoryId: Long = -1L
          var clickProductId: Long = -1L
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null
          val cityid = random.nextInt(10).toLong
          // randomly pick an action
          val action = actions(random.nextInt(4))

          // give value to corresponding actions
          action match {
            case "search" => searchKeyword = searchKeywords(random.nextInt(10))
            case "click" => clickCategoryId = random.nextInt(100).toLong
              clickProductId = String.valueOf(random.nextInt(100)).toLong
            case "order" => orderCategoryIds = random.nextInt(100).toString
              orderProductIds = random.nextInt(100).toString
            case "pay" => payCategoryIds = random.nextInt(100).toString
              payProductIds = random.nextInt(100).toString
          }

          rows += UserVisitAction(date, userid, sessionid,
            pageid, actionTime, searchKeyword,
            clickCategoryId, clickProductId,
            orderCategoryIds, orderProductIds,
            payCategoryIds, payProductIds, cityid)
        }
      }
    }
    rows.toArray
  }

  /**
    * Mock user action data
    *
    * @return
    */
  private def mockUserInfo(): Array[UserInfo] = {

    val rows = ArrayBuffer[UserInfo]()
    val sexes = Array("male", "female")
    val random = new Random()

    // generate 100 users
    for (i <- 0 to 100) {
      val userid = i
      val username = "user" + i
      val name = "name" + i
      val age = random.nextInt(60)
      val professional = "professional" + random.nextInt(100)
      val city = "city" + random.nextInt(100)
      val sex = sexes(random.nextInt(2))
      rows += UserInfo(userid, username, name, age,
        professional, city, sex)
    }
    rows.toArray
  }

  /**
    * Mock product Info
    *
    * @return
    */
  private def mockProductInfo(): Array[ProductInfo] = {

    val rows = ArrayBuffer[ProductInfo]()
    val random = new Random()
    val productStatus = Array(0, 1)

    // generate 100 product info
    for (i <- 0 to 100) {
      val productId = i
      val productName = "product" + i
      val extendInfo = "{\"product_status\": " + productStatus(random.nextInt(2)) + "}"

      rows += ProductInfo(productId, productName, extendInfo)
    }

    rows.toArray
  }

  /**
    * Insert dataframe into hive
    *
    * @param spark     sparksession
    * @param tableName tablename
    * @param dataDF    DataFrame
    */
  private def insertHive(spark: SparkSession, tableName: String, dataDF: DataFrame): Unit = {
    spark.sql("DROP TABLE IF EXISTS " + tableName)
    dataDF.write.saveAsTable(tableName)
  }

  val USER_VISIT_ACTION_TABLE = "user_visit_action"
  val USER_INFO_TABLE = "user_info"
  val PRODUCT_INFO_TABLE = "product_info"

  /**
    * entry
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    // create spark conf
    val sparkConf = new SparkConf().setAppName("MockData").setMaster("local[*]")

    // create sparksession based on conf
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // mock data
    val userVisitActionData = this.mockUserVisitActionData()
    val userInfoData = this.mockUserInfo()
    val productInfoData = this.mockProductInfo()

    // turn into rdd
    val userVisitActionRdd = spark.sparkContext.makeRDD(userVisitActionData)
    val userInfoRdd = spark.sparkContext.makeRDD(userInfoData)
    val productInfoRdd = spark.sparkContext.makeRDD(productInfoData)

    // add implicit for spark sql
    import spark.implicits._

    // insert hive
    val userVisitActionDF = userVisitActionRdd.toDF()
    insertHive(spark, USER_VISIT_ACTION_TABLE, userVisitActionDF)

    // insert hive
    val userInfoDF = userInfoRdd.toDF()
    insertHive(spark, USER_INFO_TABLE, userInfoDF)

    // insert hive
    val productInfoDF = productInfoRdd.toDF()
    insertHive(spark, PRODUCT_INFO_TABLE, productInfoDF)

    spark.close
  }

}
