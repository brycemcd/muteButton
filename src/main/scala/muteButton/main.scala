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

object StreamPrediction {
  lazy val conf = new SparkConf()
    .setAppName("muteButton")
    .setMaster("local[*]")
    .set("spark.network.timeout", "240")

  lazy val sc = new SparkContext(conf)
  val streamWindow = 2
  lazy val ssc = new StreamingContext(sc, new Duration(4000) )
  val numberOfFrequenciesCaptured = 2048
  val frequenciesInWindow = numberOfFrequenciesCaptured * streamWindow
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  def calcRatio(number : Double, divisor : Double) : Double = {
    divisor match {
      case 0 => 0.0
      case _ => number / divisor
    }
  }

  def predictFromStream = {
    val lines = ssc.socketTextStream("10.1.2.230", 9999)
    Logger.getRootLogger().setLevel(Level.ERROR)

    val stream = FrequencyIntensityStreamWithList.convertFileContentsToMeanIntensities(lines)

    stream.foreachRDD { rdd =>
      // TODO: confirm that the freq-intense batch grouping is maintained
      val allData = rdd.flatMap( outer => outer )
      val orderedData = FrequencyIntensityRDD.convertFreqIntensToLabeledPoint(allData , 3.0)
      val predictions = makePrediction(orderedData)
      val pred_cnt = predictions.count()
      val ad_ratio = calcRatio(predictions.sum, predictions.count())

      val thresh = 0.7
      println("===")
      println(s"window: $ad_ratio count: $pred_cnt")
      println("===")
      if(ad_ratio < thresh) PredictionAction.negativeCase() else PredictionAction.positiveCase()

      ssc.start()
      ssc.awaitTermination()  // Wait for the computation to terminate
      sc.stop()
    }
  }

  // NOTE: this should be created during training
  val logisticRegressionModel = loadLogisticRegressionModel("models/logreg.model-1475358050409")
  private def loadLogisticRegressionModel(path : String) = LogisticRegressionModel.load(path)

  val scalerModel : StandardScalerModel = loadScalerModel("models/scalerModel")
  private def loadScalerModel(scalerPath : String) = StandardScalerModel.load(scalerPath)

  def transformScalerModel(f : => DataFrame) = scalerModel.transform(f)
  val transformScalerModel = scalerModel.transform(_ : DataFrame)

  val predictFromDataWithDefaultModel = predictFromData( _ : DataFrame, logisticRegressionModel)
  def predictFromData(data : DataFrame, model : LogisticRegressionModel) : RDD[(Double, Vector)] = {
    model.transform(data)
      .select("rawPrediction", "prediction")
      .map {
        case Row(rawPrediction: Vector, prediction: Double) => (prediction, rawPrediction)
      }
  }


  def dataToDataFrame(data : Seq[Tuple2[Double, Vector]]) : DataFrame = sqlContext.createDataFrame(data).toDF("DONOTUSE", "rawfeatures")
  def dataToDataFrame(data : RDD[Tuple2[Double, Vector]]) : DataFrame = sqlContext.createDataFrame(data).toDF("DONOTUSE", "rawfeatures")

  def makePrediction(freqIntense : RDD[(Double, Vector)]) : RDD[Double] = {
    predictFromDataWithDefaultModel(transformScalerModel( dataToDataFrame(freqIntense) )) map(_._1)
  }
}


object Main {

  lazy val conf = new SparkConf()
    .setAppName("muteButton")
    .setMaster("local[*]")
    .set("spark.network.timeout", "240")

  lazy val sc = new SparkContext(conf)
  val streamWindow = 2
  lazy val ssc = new StreamingContext(sc, new Duration(4000) )
  val numberOfFrequenciesCaptured = 2048
  val frequenciesInWindow = numberOfFrequenciesCaptured * streamWindow
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)


  def main(args: Array[String]) = {
    sc // init it here to quiet the logs and make stopping easier
    Logger.getRootLogger().setLevel(Level.ERROR)
    //protectSanity
    //trainOfflineModel()
    //predictFromStream(PredictionAction.negativeCase, PredictionAction.positiveCase)
    //getFreqs()
    val lrm = new LogRegModel(sc, false)
    //lrm.outputPointCount(sc)
    println("done")
  }

  def trainOfflineModel() = {
    //new LogRegModel(sc, false).trainModelsWithVaryingM()
    new LogRegModel(sc, false).trainSingleModel
    sc.stop()
  }
}
