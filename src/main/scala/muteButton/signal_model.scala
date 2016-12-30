package muteButton
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{StandardScaler, PolynomialExpansion}

import org.apache.spark.ml.classification.{MultilayerPerceptronClassifier, MultilayerPerceptronClassificationModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.param.{ParamPair, ParamMap}

import org.apache.spark.mllib.evaluation.MulticlassMetrics

class SignalModel(
  val sc: SparkContext = SparkThings.sc,
  val devEnv: Boolean = true
) {

  lazy val points = SignalDataPrep.allPoints
  lazy val scaledPoints = SignalDataPrep.scaleFeatures(points, "features").cache()
  lazy val splits = scaledPoints.randomSplit(Array(0.8, 0.1, 0.1), seed = 11L)
  lazy val training = splits(0).cache()
  lazy val crossVal = splits(1).cache()
  lazy val test = splits(2).cache()
  lazy val evaluator = new MulticlassClassificationEvaluator()
    .setMetricName("f1")


  def printModelMetrics[M](m : Long,
                        trainingEval : Double,
                        cvEval : Double,
                        model : M,
                        additionalLogDetails : Array[_ <: Any],
                        appendFileName : String = "") = {

    val base = Array[String]( m.toString,
                              trainingEval.toString,
                              cvEval.toString)

    val moreAsString = additionalLogDetails.foldLeft(Array[String]()) { (acc, ele) =>
      acc ++ Array[String](ele.toString)
    }

    val logArray = base ++ moreAsString

    val strToWrite = logArray.mkString(",")
    if(appendFileName != "") scala.tools.nsc.io.File(appendFileName).appendAll(strToWrite + "\n")
    println(strToWrite)
  }
}
