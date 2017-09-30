package muteButton

import org.apache.spark.sql.DataFrame

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.param.{ParamPair, ParamMap}

import org.scalameter._

class LogRegModel (
  override val devEnv: Boolean = true
) extends SignalModel {

  val logName = "log/training-" + System.currentTimeMillis()
  val byTenPercentIncrements = (.10 to 1 by .10)
  val byFiftyPercentIncrements = (0.60 to 1 by 0.20)

  //def trainModelsWithVaryingM(seed : Long = 11L) = {
    //def calcM(allPointsCount : Long) : scala.collection.immutable.Range = {
      //val tenPer = (allPointsCount * 0.10).toInt
      //(tenPer to allPointsCount.toInt by tenPer)
    //}
    //// create an RDD of training points
    //val allPointsCount = allPoints.count()

    //val transformedData = transformToTraining(allPoints, sqlContext).cache()

    //val rangeAndStep = if(devEnv) (100 to 100 by 100) else calcM(allPointsCount)
    //for(m <- byTenPercentIncrements) {
      //val somePoints = transformedData.sample(false, m, seed)
      //trainOfflineModel(somePoints, transformToTraining, logName)
    //}
  //}

  def trainSingleModel = {
    trainOfflineModel(logName)
  }

  def trainOfflineModel(logName : String = "") = {
    // Good regParams for this model: Array[Double](0.0001, 0.0005, 0.001)
    val regRange = if(devEnv) (1 to 2) else (1 to 30)
    val lotsofRegParams = regRange.foldLeft(Array[Double](10)) {
      (acc, n) => acc :+ (acc.last/1.5)
    }

    val lr = new LogisticRegression()
      .setMaxIter(3000)
      .setFeaturesCol("scaledFeatures")

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array[Double](0.0001))
      .addGrid(lr.elasticNetParam, Array(0.0))
      .build()

    paramGrid.foreach { modelParam =>
      runTrainingAndCV(lr, modelParam, training, crossVal)
    }
  }

  def runTrainingAndCV(lr : LogisticRegression, modelParams : ParamMap, trainingData : DataFrame, cvData : DataFrame) = {

    val evaluator = new BinaryClassificationEvaluator()
        .setMetricName("areaUnderROC")

      val model = lr.fit(trainingData, modelParams)

      // TODO move this out of here
      persistModel(model)
      val trainingEval = evaluator.evaluate(
        model.transform(trainingData, modelParams))
      val cvEval = evaluator.evaluate(
        model.transform(cvData, modelParams))

      val modelParamsToPrint = printableModelParams(model)
      printModelMetrics(trainingData.count, trainingEval, cvEval, model, modelParamsToPrint, logName)
  }

  private def printableModelParams(model: LogisticRegressionModel) = {
    val interestingParams = Array[String]("elasticNetParam", "regParam")
    model.extractParamMap().toSeq.map {
      case pp : ParamPair[_] => (pp.param.name, pp.value)
    }.filter( interestingParams contains _._1 )
      .sortBy(_._1)
      .foldLeft( Array[String]() ) { (acc, n) => acc :+ n._1 + "," + n._2 }

  }

  def persistModel(model : LogisticRegressionModel) = {
      //val savableModel = lr.fit(training, model.bestModel.extractParamMap)
      val modelSaveString = "models/logreg.model-" + System.currentTimeMillis()
      model.save(modelSaveString)
  }
}
