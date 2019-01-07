package com.lxf.salestrain

import com.lxf.salestrain.DateUtil._
import com.lxf.salestrain.LoggerUtil._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}


object CreateTrain2 {

  case class User(userID: Int, age: Int, sex: Int, userGrade: Int)






  def main(args: Array[String]): Unit = {


    SetLogger
    val conf = new SparkConf().setMaster("local[8]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val path = "file:///Users/david/PycharmProjects/datafountain/dataset/"

    var goods_promote_price = spark.read.option("header", "true").format("csv").load(path + "goods_promote_price.csv")
    var goods_sku_relation = spark.read.option("header", "true").format("csv").load(path + "goods_sku_relation.csv")
    var goodsale = spark.read.option("header", "true").format("csv").load(path + "goodsale.csv")
    var goodsdaily = spark.read.option("header", "true").format("csv").load(path + "goodsdaily.csv")
    var goodsinfo = spark.read.option("header", "true").format("csv").load(path + "goodsinfo.csv")
    //var marketing = spark.read.option("header", "true").format("csv").load(path + "marketing.csv")
    val udf_date2weekNum = udf(date2weekNum _)

    //销售表
    goodsale = goodsale.withColumn("weekNum", udf_date2weekNum($"data_date"))
    //    goodsale.groupBy("weekNum").count().sort("weekNum").show(100)
    //    goodsale.show()


    goodsale = goodsale
      .groupBy("goods_id", "sku_id", "weekNum")
      .agg(Map("goods_num" -> "sum", "goods_price" -> "avg", "orginal_shop_price" -> "avg"))
        .cache()


    //每日操作记录表
    goodsdaily = goodsdaily.withColumn("weekNum", udf_date2weekNum($"data_date"))
    goodsdaily = goodsdaily.select(getCols(df = goodsdaily, strColName = Array("data_date", "goods_id")): _*)
    goodsdaily = goodsdaily
      .groupBy("weekNum", "goods_id")
      .avg("goods_click", "cart_click", "favorites_click", "sales_uv", "onsale_days")
        .cache()

    //商品推广价格表
    goods_promote_price = goods_promote_price.withColumn("weekNum", udf_date2weekNum($"data_date"))
    goods_promote_price = goods_promote_price
      .select(getCols(
        goods_promote_price,
        strColName = Array("data_date", "goods_id", "promote_start_time", "promote_end_time"),
        doubleColName = Array("promote_price", "shop_price")): _*)
    goods_promote_price = goods_promote_price
      .groupBy("weekNum", "goods_id")
      .avg("shop_price", "promote_price")
        .cache()
    //商品信息表
    //去掉两个无用的列
    goodsinfo = goodsinfo.drop("cat_level6_id", "cat_level7_id")



    goodsale
      .join(goods_promote_price, Seq("weekNum", "goods_id"), "left")
      .join(goodsdaily, Seq("weekNum", "goods_id"), "left")
      .na.fill(0)
      .withColumnRenamed("sum(goods_num)", "goods_num")
      .withColumnRenamed("avg(goods_price)", "goods_price")
      .withColumnRenamed("avg(orginal_shop_price)", "orginal_shop_price")
      .withColumnRenamed("avg(shop_price)", "shop_price")
      .withColumnRenamed("avg(promote_price)", "promote_price")
      .withColumnRenamed("avg(goods_click)", "goods_click")
      .withColumnRenamed("avg(cart_click)", "cart_click")
      .withColumnRenamed("avg(favorites_click)", "favorites_click")
      .withColumnRenamed("avg(sales_uv)", "sales_uv")
      .withColumnRenamed("avg(onsale_days)", "onsale_days")
      .write.mode("overwrite").parquet("output/train2" )
    println("保存成功：output/train2")




    spark.close()

  }

  //默认将所有列转为int型
  def getCols(df: DataFrame, strColName: Array[String] = Array(), doubleColName: Array[String] = Array()): Array[Column] = {

    var cols = df.columns.map(f => {
      if (strColName.contains(f)) {
        col(f)
      } else if (doubleColName.contains(f)) {
        col(f).cast(DoubleType)
      } else {
        col(f).cast(IntegerType)
      }
    })
    cols
  }


}
