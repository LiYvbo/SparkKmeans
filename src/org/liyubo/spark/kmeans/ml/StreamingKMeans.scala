package org.liyubo.spark.kmeans.ml

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingKMeans {

  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println(
        "Usage: StreamingKMeans " +
          "<训练集> <测试集> <批间隔时间> <族群数目> <维度数>")
      System.exit(1)
    }

    val conf = new SparkConf().setMaster("local").setAppName("3. StreamingKMeans Clustering APP")
    val ssc = new StreamingContext(conf, Seconds(args(2).toLong))

    val trainingData = ssc.textFileStream(args(0)).map(Vectors.parse)    //用于训练的输入向量流
    val testData = ssc.textFileStream(args(1)).map(LabeledPoint.parse)    //标记过的向量流作为测试

    //使用随机族群创建模型，并指定将要查找的族群数目
    val model = new StreamingKMeans()
      .setK(args(3).toInt)
      .setDecayFactor(1.0)
      .setRandomCenters(args(4).toInt, 0.0)

    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()
println("================执行到此================")
    ssc.start()
    ssc.awaitTermination()
  }
}