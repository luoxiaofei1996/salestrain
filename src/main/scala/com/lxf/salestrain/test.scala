package com.lxf.salestrain

import com.lxf.salestrain.LoggerUtil.SetLogger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    SetLogger
    val conf = new SparkConf().setMaster("local[8]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val path = "file:///Users/david/PycharmProjects/datafountain/dataset/"

    var goods_promote_price = spark.read.option("header", "true").format("csv").load(path + "goods_promote_price.csv")
    var goods_sku_relation = spark.read.option("header", "true").format("csv").load(path + "goods_sku_relation.csv")
    var goodsale = spark.read.option("header", "true").format("csv").load(path + "goodsale.csv")
    var goodsdaily = spark.read.option("header", "true").format("csv").load(path + "goodsdaily.csv")
    var goodsinfo = spark.read.option("header", "true").format("csv").load(path + "goodsinfo.csv")
    println(goods_sku_relation.groupBy("sku_id").count().count())
    goods_sku_relation.groupBy("goods_id").count().show()
  }
}
