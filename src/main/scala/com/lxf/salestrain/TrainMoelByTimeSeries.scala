package com.lxf.salestrain

import com.lxf.salestrain.DateUtil._
import com.lxf.salestrain.LoggerUtil._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object TrainMoelByTimeSeries {

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
    //    goodsale.groupBy("weekNum").count().sort("weekNum").show(100)
    //    goodsale.show()



    goodsale = goodsale
      .groupBy("goods_id", "sku_id", "weekNum")
      .agg(Map("goods_num" -> "sum", "goods_price" -> "avg", "orginal_shop_price" -> "avg"))

//    val timeSeriesModel=new TimeSeriesModel(19,"")


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
