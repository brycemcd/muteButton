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
import org.apache.spark.ml.feature.StandardScalerModel

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
  //lazy val ssc = new StreamingContext(sc, Seconds(streamWindow))
  lazy val ssc = new StreamingContext(sc, new Duration(500) )
  val numberOfFrequenciesCaptured = 2048
  val frequenciesInWindow = numberOfFrequenciesCaptured * streamWindow
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)


  def main(args: Array[String]) = {
    sc // init it here to quiet the logs and make stopping easier
    Logger.getRootLogger().setLevel(Level.ERROR)
    //protectSanity
    //trainOfflineModel()
    predictFromStream(PredictionAction.negativeCase, PredictionAction.positiveCase)
    //getFreqs()
    val lrm = new LogRegModel(sc, false)
    //lrm.outputPointCount(sc)
    println("done")
  }
  private def generateModelParams : Seq[SGDModelParams] = {
    // NOTE: I couldn't find in my notes if these were sensible defaults
    val regs = Seq[Double](0.01, 0.1)
    for(regParam <- regs;
         numIterations <- (3000 to 3000 by 10) ) yield SGDModelParams(regParam, numIterations)
  }

  def trainOfflineModel() = {
    //new LogRegModel(sc, false).trainModelsWithVaryingM()
    new LogRegModel(sc, false).trainSingleModel
    sc.stop()
  }

  import sqlContext.implicits._
  def predictFromStream(negativeAction : () => Int,
                        positiveAction : () => Int,
                        modelPath: String = "models/logreg.model-1475358050409",
                        scalerPath: String = "models/scalerModel") = {

    // NOTE: I may want to broadcast this
    val model = LogisticRegressionModel.load(modelPath)

    val lines = ssc.socketTextStream("10.1.2.230", 9999)
    Logger.getRootLogger().setLevel(Level.ERROR)

    val meanByKey = FrequencyIntensityStream.convertFileContentsToMeanIntensities(lines)

    // FIXME: This is very dangerous
    val zerosArray = scala.collection.mutable.ArrayBuffer.empty[Double]
    meanByKey.foreachRDD { mbk =>

      // FIXME: there must be a better way to make this usable as a dataframe
      val predictData = (3.0, Vectors.dense( mbk.map(_._2).collect ))
      val predictVec = Vectors.dense( mbk.map(_._2).collect )

      val sz = predictData._2.size
      if(sz == numberOfFrequenciesCaptured) {
        val predictPointsDF = sqlContext.createDataFrame(Seq(predictData)).toDF("DONOTUSE", "rawfeatures")
        // NOTE: this should be created during training
        val transformedData = StandardScalerModel.load(scalerPath).transform(predictPointsDF)

        val predictLabelAndRaw = model.transform(transformedData)
          .select("rawPrediction", "prediction")
          .map {
            case Row(rawPrediction: Vector, prediction: Double) => (prediction, rawPrediction)
          }
        val prediction = predictLabelAndRaw.first()._1
        //predictLabelAndRaw.foreach(println)
        //println("prediction: ---- " + prediction + " ----")
        //if(prediction == 0) negativeAction() else positiveAction()
        zerosArray += prediction
      } else {
        println("cannot predict, vector size not 2048: " + sz)
        zerosArray += 0.0
      }

      // Every 5 seconds output the sum of ad predictions
      val window = 10
      if( (zerosArray.length % window) == 0 ) {
        val thresh = 7.0
        val value = zerosArray.takeRight(window).sum
        println("window: " + value)
        if(value < thresh) negativeAction() else positiveAction()
      }
    }

    ssc.start()
    ssc.awaitTermination()  // Wait for the computation to terminate
    sc.stop()
  }
}
