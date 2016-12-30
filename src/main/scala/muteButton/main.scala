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

import muteButton.NewTypes._

object SparkThings {
  val conf = new SparkConf()
    .setAppName("muteButton")
    .setMaster("local[*]")
    .set("spark.network.timeout", "240")

  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, new Duration(4000) )
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
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


  def main(args: Array[String]) = {
    //sc // init it here to quiet the logs and make stopping easier
    // TODO: uncomment this to predict
    StreamPrediction.processStream
    // TODO: uncomment this for offline training
    //new NNModel(devEnv = false).singleNNModel
    //
    //new GBTModel(devEnv = true).singleGBTModel
    //
    //FoolingAround.writeSummaryToFile
    //protectSanity
    //trainOfflineModel()
    //getFreqs()
    //val lrm = new LogRegModel(sc, false)
    //lrm.outputPointCount(sc)
    sc.stop()
    println("done")
  }

  def trainOfflineModel() = {
    //new LogRegModel(sc, false).trainModelsWithVaryingM()
    //new LogRegModel(sc, false).trainSingleModel
  }
}
