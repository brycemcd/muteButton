package muteButton

import muteButton.NewTypes._

import org.apache.spark.ml.classification.{LogisticRegressionModel}
import org.apache.spark.ml.feature.StandardScalerModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object StreamPrediction {
  val conf = SparkThings.conf
  val sc = SparkThings.sc
  val ssc = SparkThings.ssc
  val sqlContext = SparkThings.sqlContext
  val streamWindow = 2
  val numberOfFrequenciesCaptured = 2048
  val frequenciesInWindow = numberOfFrequenciesCaptured * streamWindow

  def calcRatio(number : Double, divisor : Double) : Double = {
    divisor match {
      case 0 => 0.0
      case _ => number / divisor
    }
  }

  def convertProcessedStreamToDataPoints(dataPoints : RDD[LabeledFreqIntens]) : RDD[PredictTuple] = {
    FrequencyIntensityRDD.convertFreqIntensToLabeledPoint(dataPoints , 3.0)
  }

  def flatMapsIt(rdd : RDD[List[(String, (Double, Double))]]) : RDD[LabeledFreqIntens] = rdd.flatMap { x => x }

  // NOTE: I found this helpful: https://twitter.github.io/scala_school/pattern-matching-and-functional-composition.html
  def makePredictionFromStreamedRDD = {
    flatMapsIt _ andThen
      convertProcessedStreamToDataPoints _ andThen
      dataToDataFrame _ andThen
      transformScalerModel _ andThen
      predictFromDataWithDefaultModel _ andThen
      extractPredictions _
  }

  def calculatePredictionRatio(predictions: RDD[Double]) : (Double, Double) = {
      val pred_cnt = predictions.count()
      val ad_ratio = calcRatio(predictions.sum, predictions.count())
      (pred_cnt, ad_ratio)
  }

  def printPredictions(ad_ratio : Double, pred_cnt : Double) : Unit = {
      println("===")
      println(s"window: $ad_ratio count: $pred_cnt")
      println("===")
  }

  def predictFromStream(rdd : RDD[List[LabeledFreqIntens]]) =  {
      // TODO: confirm that the freq-intense batch grouping is maintained
      val predictions = makePredictionFromStreamedRDD(rdd)
      val (pred_cnt, ad_ratio) = calculatePredictionRatio(predictions)

      printPredictions(ad_ratio, pred_cnt)
      PredictionAction.ratioBasedMuteAction(0.7, ad_ratio)
  }

  def processStream = {
    val lines = ssc.socketTextStream("10.1.2.230", 9999)

    FrequencyIntensityStreamWithList.convertFileContentsToMeanIntensities(lines)
       .foreachRDD { rdd => predictFromStream(rdd) }

    ssc.start()
    ssc.awaitTermination()  // Wait for the computation to terminate
    sc.stop()
  }

  // NOTE: this should be created during training
  val logisticRegressionModel : String => LogisticRegressionModel = LogisticRegressionModel.load(_ : String)
  val defaultLogisticRegressionModel : LogisticRegressionModel = logisticRegressionModel("models/logreg.model-1475358050409")

  val scalerModel : String => StandardScalerModel = StandardScalerModel.load( _ : String)
  val defaultScalerModel : StandardScalerModel = scalerModel("models/scalerModel")

  def transformScalerModel(df : DataFrame) : DataFrame = defaultScalerModel.transform(df)

  def predictFromDataWithDefaultModel(df : DataFrame) = predictFromData( df : DataFrame, defaultLogisticRegressionModel)
  def predictFromData(data : DataFrame, model : LogisticRegressionModel) : RDD[(Double, Vector)] = {
    model.transform(data)
      .select("rawPrediction", "prediction")
      .map {
        case Row(rawPrediction: Vector, prediction: Double) => (prediction, rawPrediction)
      }
  }


  def dataToDataFrame(data : Seq[Tuple2[Double, Vector]]) : DataFrame = sqlContext.createDataFrame(data).toDF("DONOTUSE", "rawfeatures")
  def dataToDataFrame(data : RDD[Tuple2[Double, Vector]]) : DataFrame = sqlContext.createDataFrame(data).toDF("DONOTUSE", "rawfeatures")

  def extractPredictions(data: RDD[(Double, Vector)]) : RDD[Double] = data map(_._1)
}
