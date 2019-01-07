package com.lxf.salestrain

import com.lxf.salestrain.LoggerUtil.SetLogger
import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostModel}
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession


object Output {
  def main(args: Array[String]): Unit = {
    SetLogger
    val conf = new SparkConf().setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val model = XGBoostModel.load("output/model")
    val path = "file:///Users/david/PycharmProjects/datafountain/dataset/"
    var submit=spark.read.option("header","true").option("sep",",").csv(path+"submit_example.csv")
    submit =submit.drop("week1","week2","week3","week4","week5")
    var goods_sku_relation = spark.read.option("header", "true").format("csv").load(path + "goods_sku_relation.csv")


    var train_52 = spark.read.parquet("output/train_52")
    var train_51 = spark.read.parquet("output/train_51")
    var train_50 = spark.read.parquet("output/train_50")
    var train_49 = spark.read.parquet("output/train_49")
    var train_48 = spark.read.parquet("output/train_48")
    var train_7 = spark.read.parquet("output/train_7")

    val cols = train_48.columns.filter(!Array("sku_id","goods_id", "label").contains(_))
    val vec = new VectorAssembler().setInputCols(cols).setOutputCol("features")
    train_52 = vec.transform(train_52)
    train_51 = vec.transform(train_51)
    train_50 = vec.transform(train_50)
    train_49 = vec.transform(train_49)
    train_48 = vec.transform(train_48)

    import spark.implicits._
    val week1 = model.transform(train_52).select($"sku_id", $"prediction" as "week1").join(submit,Seq("sku_id"),"right")
    val week2 = model.transform(train_51).select($"sku_id", $"prediction" as "week2").join(submit,Seq("sku_id"),"right")
    val week3 = model.transform(train_50).select($"sku_id", $"prediction" as "week3").join(submit,Seq("sku_id"),"right")
    val week4 = model.transform(train_49).select($"sku_id", $"prediction" as "week4").join(submit,Seq("sku_id"),"right")
    val week5 = model.transform(train_48).select($"sku_id", $"prediction" as "week5").join(submit,Seq("sku_id"),"right")

    println(week1.where($"week1".isNull).count())
    println(week1.where($"week1".isNotNull).count())



    submit=submit.drop("week1","week2","week3","week4","week5")

//    val result = week1
//      .join(week2, Seq("goods_id"), "outer")
//      .join(week3, Seq("goods_id"), "outer")
//      .join(week4, Seq("goods_id"), "outer")
//      .join(week5, Seq("goods_id"), "outer")
//    result.show()

    spark.close()
  }
}
