package muteButton

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector

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

import muteButton.NewTypes._

object SparkThings {
  val conf = new SparkConf()
    .setAppName("muteButton")
    .setMaster("local[2]")
    .set("spark.network.timeout", "240")

  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, new Duration(4000) )
  //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val sqlContext = SparkSession.builder
    .master("local[2]")
    .appName("muteButton")
    .getOrCreate()
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

}



object Main {
  //lazy val conf = SparkThings.conf
  lazy val sc = SparkThings.sc
  val streamWindow = 2
  //lazy val ssc = SparkThings.ssc
  lazy val numberOfFrequenciesCaptured = 2048
  lazy val frequenciesInWindow = numberOfFrequenciesCaptured * streamWindow
  //lazy val sqlContext = SparkThings.sqlContext


  //private def streamProcessing() = {
    //StreamPrediction.processStream
  //}

  private def trainNNModel() = {
    new NNModel(devEnv = false).singleNNModel
  }

  def main(args: Array[String]) = {
    sc // init it here to quiet the logs and make stopping easier
    // TODO: uncomment this to predict
    //streamProcessing()
    // TODO: uncomment this for offline training
    //new NNModel(devEnv = false).singleNNModel
    //
    //new GBTModel(devEnv = true).singleGBTModel
    //
    //FoolingAround.writeSummaryToFile
    //protectSanity
    //trainOfflineModel()
    //getFreqs()

    trainLogRegModel

    //lrm.outputPointCount(sc)
    sc.stop()
    println("done")
  }

  def trainLogRegModel = {
    val lrm = new LogRegModel(true)
    lrm.trainSingleModel
  }

  def trainOfflineModel() = {
    //new LogRegModel(sc, false).trainModelsWithVaryingM()
    //new LogRegModel(sc, false).trainSingleModel
  }
}
