package com.lxf.salestrain

import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostModel}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.evaluation.RegressionEvaluator
import com.lxf.salestrain.LoggerUtil._
object TrainModel {
  def main(args: Array[String]): Unit = {
    SetLogger
    val conf =new SparkConf().setMaster("local[*]")
    val spark =SparkSession.builder().config(conf).getOrCreate()
    import spark.sql
    var pre_train = spark.read.parquet("output/train")
    val cols=pre_train.columns.filter(!Array("sku_id","goods_id","label").contains(_))
    pre_train=new VectorAssembler().setInputCols(cols).setOutputCol("features").transform(pre_train)

    val Array(train,test) =pre_train.randomSplit(Array(0.8,0.2))
    val(maxDepth , numRound , nworker )=(7,7,7)
    val paramMap = List(
      "eta" -> 0.1, //学习率
      "gamma" -> 0.1, //用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
      "lambda" -> 2, //控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
      "subsample" -> 1, //随机采样训练样本
      "colsample_bytree" -> 0.8, //生成树时进行的列采样
      "max_depth" -> maxDepth, //构建树的深度，越大越容易过拟合
      "min_child_weight" -> 5,
      "objective" -> "reg:linear" //定义学习任务及相应的学习目标
    ).toMap

    train.show(false)

    println("------------开始模型训练")
    val model: XGBoostModel = XGBoost.trainWithDataFrame(train, paramMap, numRound, nworker,
      useExternalMemory = true,
      featureCol = "features",
      labelCol = "label",
      missing = 0.0f)
    model.write.overwrite().save("output/model")
    val result=model.transform(test)
    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("label").setPredictionCol("prediction")
    val x=evaluator.evaluate(result)
    println(1.0/(x+1))
    println(model.summary)

    println("'")
    spark.close()
  }
}
