package com.lxf.salestrain

import com.lxf.salestrain.LoggerUtil._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import com.lxf.salestrain.DateUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}


object AnalysisData {

  case class User(userID: Int, age: Int, sex: Int, userGrade: Int)

  def getDfByWeek(spark: SparkSession
                  , goodsale: DataFrame, goods_promote_price: DataFrame, goodsdaily: DataFrame,
                  week: Int): DataFrame = {
    import spark.implicits._
    val goodsale_60 = goodsale.filter($"weekNum" === week)
    val goods_promote_price_60 = goods_promote_price.filter($"weekNum" === week)
    val goodsdaily_60 = goodsdaily.filter($"weekNum" === week)

    return goodsale_60
      .join(goods_promote_price_60, Seq("weekNum", "goods_id"), "left")
      .join(goodsdaily_60, Seq("weekNum", "goods_id"), "left")

  }

  def main(args: Array[String]): Unit = {
    SetLogger
    val conf = new SparkConf().setMaster("local[4]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.sql
    import spark.implicits._


    val path = "file:///Users/david/PycharmProjects/datafountain/dataset/"

    var goods_promote_price = spark.read.option("header", "true").format("csv").load(path + "goods_promote_price.csv")
    var goods_sku_relation = spark.read.option("header", "true").format("csv").load(path + "goods_sku_relation.csv")
    var goodsale = spark.read.option("header", "true").format("csv").load(path + "goodsale.csv")
    var goodsdaily = spark.read.option("header", "true").format("csv").load(path + "goodsdaily.csv")
    var goodsinfo = spark.read.option("header", "true").format("csv").load(path + "goodsinfo.csv")
    //var marketing = spark.read.option("header", "true").format("csv").load(path + "marketing.csv")
    var submit=spark.read.option("header","true").option("sep",",").csv(path+"submit_example.csv")
    submit =submit.drop("week1","week2","week3","week4","week5")

    val udf_date2weekNum = udf(date2weekNum _)

    //销售表
    goodsale = goodsale.withColumn("weekNum", udf_date2weekNum($"data_date"))
        goodsale.groupBy("weekNum").count().sort("weekNum").show(100)
        goodsale.show()



    goodsale = goodsale
      .groupBy("goods_id", "sku_id", "weekNum")
      .agg(Map("goods_num" -> "sum", "goods_price" -> "avg", "orginal_shop_price" -> "avg"))


//    println(submit.join(goodsale.groupBy("sku_id").count(), Seq("sku_id"), "inner").count())
    //每日操作记录表
    goodsdaily = goodsdaily.withColumn("weekNum", udf_date2weekNum($"data_date"))
    goodsdaily = goodsdaily.select(getCols(df = goodsdaily, strColName = Array("data_date", "goods_id")): _*)

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
    //商品信息表
    //去掉两个无用的列
    goodsinfo = goodsinfo.drop("cat_level6_id", "cat_level7_id")


    /**
      * 第60周 ->第7周
      * 第59周 ->第6周
      */

//    println(goodsale.count())
    goodsale.join(submit, Seq("sku_id"), "inner").groupBy("weekNum").sum("sum(goods_num)").orderBy($"weekNum".desc).show(100)


    goodsale.join(submit, Seq("sku_id"), "inner").groupBy("weekNum").count().orderBy($"weekNum".desc).show(100)


    val frame_60 = getDfByWeek(spark, goodsale, goods_promote_price, goodsdaily, 60).drop("weekNum")
    val frame_59 = getDfByWeek(spark, goodsale, goods_promote_price, goodsdaily, 59).drop("weekNum")
    val sale_7 = goodsale.filter($"weekNum" === 7).select($"sku_id", $"sum(goods_num)" as "label")
    val sale_6 = goodsale.filter($"weekNum" === 6).select($"sku_id", $"sum(goods_num)" as "label")

    val train1 = frame_59.join(sale_6, Seq("sku_id"), "left").na.fill(0)
    val train2 = frame_60.join(sale_7, Seq("sku_id"), "left").na.fill(0)
    var train = train1.union(train2)
    train = train
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
//    train.write.mode("overwrite").parquet("output/train")

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
