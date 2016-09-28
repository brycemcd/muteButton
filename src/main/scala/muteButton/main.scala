package muteButton

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.dstream.DStream

//import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector

//import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD,LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint


import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
//import sqlContext.implicits._
import org.apache.spark.ml.feature.StandardScaler

import org.apache.log4j.Logger
import org.apache.log4j.Level

// spark.ml
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegressionModel}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType};

import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}

// NOTE: I just lifted this out of ml-playground. Remove it and just use
// the playground later. I really should be using it here but I'm on a roll
case class SGDModelParams( regParam: Double,
                           numIterations: Int) {

}

object Main {

  lazy val conf = new SparkConf()
    .setAppName("muteButton")
    .setMaster("local[*]")
    .set("spark.network.timeout", "240")

  lazy val sc = new SparkContext(conf)
  val streamWindow = 2
  lazy val ssc = new StreamingContext(sc, Seconds(streamWindow))
  val numberOfFrequenciesCaptured = 1024
  val frequenciesInWindow = numberOfFrequenciesCaptured * streamWindow


  def main(args: Array[String]) = {
    sc // init it here to quiet the logs and make stopping easier
    Logger.getRootLogger().setLevel(Level.ERROR)
    //protectSanity
    trainOfflineModel()
    //predictFromStream( PredictionAction.negativeCase,
                       //PredictionAction.positiveCase)
    //getFreqs()
  }
  private def generateModelParams : Seq[SGDModelParams] = {
    // NOTE: I couldn't find in my notes if these were sensible defaults
    val regs = Seq[Double](0.01, 0.1)
    for(regParam <- regs;
         numIterations <- (3000 to 3000 by 10) ) yield SGDModelParams(regParam, numIterations)
  }

  def trainOfflineModel() = {
    new LogRegModel(sc, true).trainModelWithAllData()
    sc.stop()
  }

  def predictFromStream(negativeAction : () => Int,
                        positiveAction : () => Int) = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val modelPath = "models/logreg.model-1474225317257"
    val model = LogisticRegressionModel.load(modelPath)

    val lines = ssc.socketTextStream("10.1.2.230", 9999)
    Logger.getRootLogger().setLevel(Level.ERROR)

    val meanByKey = FrequencyIntensityStream.convertFileContentsToMeanIntensities(lines)

    meanByKey.foreachRDD { mbk =>

      val predictData = (0.0, Vectors.dense( mbk.map(_._2).take( frequenciesInWindow ) ))

      val sz = predictData._2.size
      if(sz == frequenciesInWindow) {
        val predictPointsDF = sqlContext.createDataFrame(Seq(predictData)).toDF("DO_NOT_USE", "rawfeatures")
        val scaler = new StandardScaler()
          .setInputCol("rawfeatures")
          .setOutputCol("features")
          .setWithStd(true)
          .setWithMean(false)

        val scalerModel = scaler.fit(predictPointsDF)
        val transformedData = scalerModel.transform(predictPointsDF)

        val predictLabelAndRaw = model.transform(transformedData)
          .select("rawPrediction", "prediction")
          .map {
            case Row(rawPrediction: Vector, prediction: Double) => (prediction, rawPrediction)
          }
        predictLabelAndRaw.foreach(println)
        val prediction = predictLabelAndRaw.first()._1
        println("prediction: ---- " + prediction + " ----")
        if(prediction == 0) negativeAction() else positiveAction()
      } else {
        println("cannot predict, vector size not 2048: " + sz)
      }
    }

    ssc.start()
    ssc.awaitTermination()  // Wait for the computation to terminate
    sc.stop()
  }
}
