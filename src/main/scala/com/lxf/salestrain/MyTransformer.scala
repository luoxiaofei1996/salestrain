package com.lxf.salestrain

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer

class MyTransformer(override val uid: String) extends Transformer {
  def this() = this(Identifiable.randomUID("myTransformer"))



   var spark: SparkSession =_

   var cols: Array[String] =_
   var timeCol: String =_
   var idCol: String =_
   var targetTime: Int =_
   var statisticTime: Int = 30


  def setCols(cols: Array[String]):this.type ={
    this.cols=cols
    this
  }
  def setTimeCol(timeCol: String):this.type ={
    this.timeCol=timeCol
    this
  }
  def setIdCol(idCol: String):this.type ={
    this.idCol=idCol
    this
  }
  def setTargetTime(targetTime: Int):this.type ={
    this.targetTime=targetTime
    this
  }
  def setStatisticTime(statisticTime: Int):this.type ={
    this.statisticTime=statisticTime
    this
  }




  override def transform(df: Dataset[_]): DataFrame = {
//    df.groupByKey(x=>df(idCol,timeCol))(Encoder[Column]).mapGroups((ids,ite)=>ids)(Encoder[Column])

    import df.sparkSession.implicits._
    val colsSize = cols.length
    val rdd=df
        .toDF()
      .rdd
      .map(x => {
        val array = new ArrayBuffer[Double]
        cols.foreach(col => array.append(x.get(x.fieldIndex(col)).toString.toDouble))
        val key = x.get(x.fieldIndex(idCol)).toString
        val value = Map(x.get(x.fieldIndex(timeCol)).toString.toInt -> array.toArray)
        (key, value)
      })
      .combineByKey(
        (v) => v,
        (acc: Map[Int, Array[Double]], v: Map[Int, Array[Double]]) => acc ++ v,
        (acc1: Map[Int, Array[Double]], acc2: Map[Int, Array[Double]]) => acc1 ++ acc2
      )
      .map(x => {
        //id
        val id = x._1
        //某个id的统计值
        val map = x._2
        //将所有的特征保存在array中
        var features = ArrayBuffer[Double]()
        //计算前elem个单位的统计量和
        var sum = new Array[Double](colsSize)
        for (elem <- 0 to 30) {
          val feature = map.get(targetTime - elem).getOrElse(new Array[Double](colsSize))
          for (i <- Range(0, colsSize)) {
            sum(i) += feature(i)
          }
          features.appendAll(sum)
        }
        (id, Vectors.dense(features.toArray))
      })
      rdd.toDF("id","features")

  }



  override def copy(extra: ParamMap): Transformer = ???

  override def transformSchema(schema: StructType): StructType = ???

}
