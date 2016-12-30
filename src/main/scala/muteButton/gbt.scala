package muteButton

import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}

import org.apache.spark.ml.tuning.{ParamGridBuilder}
import org.apache.spark.ml.param.{ParamPair, ParamMap}
import org.apache.spark.sql.DataFrame

class GBTModel(override val devEnv: Boolean = true) extends SignalModel {

  lazy val modelName = "gbtmodel"
  lazy private val logName = "log/" + modelName + "-" + System.currentTimeMillis()

  val baseModel = new GBTClassifier().setMaxIter(10).setFeaturesCol("features").setLabelCol("label")

  def singleGBTModel = {
    trainOfflineModel(scaledPoints)
  }

  val paramGrid = new ParamGridBuilder()
    .addGrid(baseModel.maxIter, Array[Int](10, 30)).build()

  def trainOfflineModel(allPoints: DataFrame) = {
    paramGrid.foreach { modelParam =>
      runTrainingAndCV(baseModel, modelParam)
    }
  }

  def runTrainingAndCV[GBTClassificationModel](mp : GBTClassifier,
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
                         Array[Int](),
                         logName)
  }
}
