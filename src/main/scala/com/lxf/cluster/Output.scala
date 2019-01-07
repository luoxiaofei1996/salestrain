package com.lxf.cluster

import ml.dmlc.xgboost4j.scala.spark.XGBoostModel
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import com.lxf.salestrain.LoggerUtil._

object Output {

  def output(spark: SparkSession,submit: DataFrame, train_left: DataFrame, model: XGBoostModel,weekNum:Int) = {
    import spark.implicits._
    val submit2=submit
      .drop("week2", "week3", "week4", "week5")
      .select($"sku_id", col("week1").cast(IntegerType) + 62 as "weekNum")

    var week1=submit2.join(train_left,Seq("sku_id"),"left")
    val trans=new VectorAssembler().setInputCols(Array("features","weekNum")).setOutputCol("features2")

    week1=trans.transform(week1)
    week1 = model
      .transform(week1)
      .select($"sku_id",(col("prediction")+0.5).cast(IntegerType) as "prediction")
      .withColumnRenamed("prediction","sale")

    week1
  }

  def main(args: Array[String]): Unit = {
    SetLogger
    val conf = new SparkConf().setMaster("local[9]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val path = "file:///Users/david/PycharmProjects/datafountain/dataset/"
    var train_left = spark.read.parquet("output/train_left")

    var submit = spark.read.option("header", "true").option("sep", ",").csv(path + "submit_example.csv")
    val model=XGBoostModel.load("output/model")


    val week1=output(spark,submit,train_left,model,62).withColumnRenamed("sale","week1")
    val week2=output(spark,submit,train_left,model,63).withColumnRenamed("sale","week2")
    val week3=output(spark,submit,train_left,model,64).withColumnRenamed("sale","week3")
    val week4=output(spark,submit,train_left,model,65).withColumnRenamed("sale","week4")
    val week5=output(spark,submit,train_left,model,66).withColumnRenamed("sale","week5")



    val result=week1.join(week2,Seq("sku_id"))
      .join(week3,Seq("sku_id"))
      .join(week4,Seq("sku_id"))
      .join(week5,Seq("sku_id"))
      result.show()
    println(result.count())

    result.repartition(1).write.mode("overwrite").option("header","true").option("sep",",").format("csv").save("output/result")






  }
}
