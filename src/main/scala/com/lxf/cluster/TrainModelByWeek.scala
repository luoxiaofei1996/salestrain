package com.lxf.cluster

import com.lxf.salestrain.LoggerUtil.SetLogger
import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostModel}
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object TrainModelByWeek {

  case class SkuWeekInfo(sku_id: String, goods_num: Double, goods_price: Double, orginal_shop_price: Double, shop_price: Double, promote_price: Double, goods_click: Double, cart_click: Double, favorites_click: Double, sales_uv: Double, onsale_days: Double, weekNum2:Int)

  def main(args: Array[String]): Unit = {
    if(args.length!=3){
      System.err.print("Params: <filePath> <modelSavePath> <numRound>")
      System.exit(-1)
    }

    val filePath=args(0)
    val modelSavePath=args(1)
//    SetLogger
    val conf = new SparkConf()//.setMaster("local[9]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    var train = spark.read.parquet(filePath)
    import spark.implicits._
    train = train.withColumn("weekNum2", -col("weekNum") + 61).drop("weekNum").drop("goods_id")


    val train2 = train.cache()
    val train_left=train2
      .as[SkuWeekInfo]
        .rdd
        .map(x=>
          (x.sku_id,Array(x.weekNum2,x.goods_num,x.goods_price,x.orginal_shop_price,x.shop_price,x.promote_price,x.goods_click,x.cart_click,x.favorites_click,x.sales_uv,x.onsale_days))
        )
        .groupByKey()
        .map(
          x=>{
            import scala.collection.mutable.Map
            val map:Map[Int,Array[Double]]=Map()
            for (elem <- x._2) {
              map.put(elem(0).toInt, elem)
            }
            import scala.collection.mutable.ArrayBuffer
            val array:ArrayBuffer[Double]=ArrayBuffer()
            for(i<- 1 to 55){
              if(map.contains(i)){
                array.appendAll(map(i))
              }else{
                array.appendAll(Array(0.0,0,0,0,0,0,0,0,0,0,0))
              }
            }
            (x._1,Vectors.dense(array.toArray))
          }
        )
        .toDF("sku_id","features")
//        .show(false)

    train_left.write.mode("overwrite").parquet("output/train_right")
    val train_right=train.select("sku_id","weekNum2","goods_num")
    var train_union=train_left.join(train_right,Seq("sku_id"),"left")

    train_union=new VectorAssembler().setInputCols(Array("features","weekNum2")).setOutputCol("features2").transform(train_union)


    val Array(trainl,testl) =train_union.randomSplit(Array(0.8,0.2))
    val(maxDepth , numRound , nworker )=(7,args(2).toInt,7)
    val paramMap = List(
      "eta" -> 0.01, //学习率
      "gamma" -> 0.1, //用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
      "lambda" -> 2, //控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
      "subsample" -> 1, //随机采样训练样本
      "colsample_bytree" -> 0.8, //生成树时进行的列采样
      "max_depth" -> maxDepth, //构建树的深度，越大越容易过拟合
      "min_child_weight" -> 5,
      "objective" -> "reg:linear"  //定义学习任务及相应的学习目标
    ).toMap
    val model: XGBoostModel = XGBoost.trainWithDataFrame(trainl, paramMap, numRound, nworker,
      useExternalMemory = true,
      featureCol = "features2",
      labelCol = "goods_num",
      missing = 0.0f)

    model.write.overwrite().save(modelSavePath)

    val result = model.transform(testl)
    result.select("goods_num","prediction").show(truncate = false,numRows = 100)

    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("goods_num").setPredictionCol("prediction")
    val x=evaluator.evaluate(result)
    println(1.0/(x+1))





  }

}


