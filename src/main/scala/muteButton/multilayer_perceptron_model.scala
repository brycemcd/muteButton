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

import org.scalameter._

class NNModel(override val devEnv: Boolean = true) extends SignalModel {

  lazy val modelName = "nnmodel"
  lazy private val logName = "log/" + modelName + "-" + System.currentTimeMillis()

  val baseModel = new MultilayerPerceptronClassifier()
    .setSeed(1234L)
    .setMaxIter(100)

  def singleNNModel = {
    trainOfflineModel(scaledPoints)
  }

  val paramGrid = new ParamGridBuilder()
    .addGrid(baseModel.layers, Array(
      Array[Int](2048, 5, 4, 2),
      Array[Int](2048, 100, 2),
      Array[Int](2048, 100, 100, 2),
      Array[Int](2048, 100, 100, 100, 2),
      Array[Int](2048, 200, 2),
      Array[Int](2048, 400, 2),
      Array[Int](2048, 600, 2)
      //Array[Int](2048, 4096, 2) # NOTE: does not complete on one box
    ))
      .build()
  def trainOfflineModel(allPoints: DataFrame) = {


    paramGrid.foreach { modelParam =>
      runTrainingAndCV(baseModel, modelParam)
    }
  }

  def runTrainingAndCV(mp : MultilayerPerceptronClassifier,
                       modelParams : ParamMap) = {

      val model = mp.fit(training, modelParams)
      // TODO move this out of here
      // persistModel(model)
      val trainingEval = evaluator.evaluate(
        model.transform(training, modelParams))
      val cvEval = evaluator.evaluate(
        model.transform(crossVal, modelParams))

      printModelMetrics( training.count,
                         trainingEval,
                         cvEval,
                         model,
                         model.layers,
                         logName)
  }
}
