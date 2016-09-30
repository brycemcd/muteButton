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
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.param.{ParamPair, ParamMap}

import org.apache.spark.mllib.evaluation.MulticlassMetrics

object LogRegModel {
  def trainOfflineModelNEW(sc : SparkContext) = {
    // TODO figure out how to save a file of labeled points
    val allPoints = sc.textFile("hdfs://spark3.thedevranch.net/football/allTrainingPoints").map {
      case line =>
        val sp = line.split(",")
        (sp(0), sp(1))
    }.take(1).foreach(println)
  }
}

class LogRegModel(
  sc: SparkContext,
  devEnv: Boolean = true
) {

  lazy private val sqlContext = new SQLContext(sc)

  private def deriveAllPointsFromLabeledFreqs(sc : SparkContext) : RDD[(Double, Vector)] = {
    val adfile = if(devEnv) "/media/brycemcd/filestore/spark2bkp/football/supervised_samples/ad/freqs/ari_phi_chunked091_freqs-labeled.txt" else "hdfs://spark3.thedevranch.net/football/freqs/ad/all-labeled.txt"
    val gamefile = if(devEnv) "/media/brycemcd/filestore/spark2bkp/football/supervised_samples/game/freqs/ari_phi_chunked095_freqs-labeled.txt" else "hdfs://spark3.thedevranch.net/football/freqs/game/all-labeled.txt"
    val trainAdLines = sc.textFile(adfile)
    val trainGameLines = sc.textFile(gamefile)
    //println("training ad lines " + trainAdLines.count())
    //println("training game lines " + trainGameLines.count())

    val trainAdTouples = FrequencyIntensityRDD.convertFileContentsToMeanIntensities(trainAdLines)
    val trainGameTouples = FrequencyIntensityRDD.convertFileContentsToMeanIntensities(trainGameLines)

    //println("training ad touples " + trainAdTouples.count())
    //println("training game touples " + trainGameTouples.count())


    // NOTE: I should start with a collection of (filename, (freq, intensity)) tuples
    // NOTE: There's a slight risk that frequencies and intensity examples will
    // get mixed up when this is distributed across a cluster

    val trainAdPoints = trainAdTouples.groupByKey().map(_._2).map(_.toArray.sortBy(_._1)).map { x =>
      (1.0, Vectors.dense( x.map(_._2) ))
    }


    val trainGamePoints = trainGameTouples.groupByKey().map(_._2).map(_.toArray.sortBy(_._1)).map { x =>
      (0.0, Vectors.dense( x.map(_._2) ))
    }


    //println("training ad points " + trainAdPoints.count())
    //println("training game points " + trainGamePoints.count())

    trainGamePoints.union(trainAdPoints)
  }

  def scaleFeatures(allPoints: RDD[(Double, Vector)],
                          sqlContext: SQLContext,
                          scaledColumnName : String) = {

    val allPointsDF = sqlContext.createDataFrame(allPoints)
      .toDF("label", "rawfeatures")

    val scaler = new StandardScaler()
      .setInputCol("rawfeatures")
      .setOutputCol(scaledColumnName)
      .setWithStd(true)
      .setWithMean(false)

    val scalerModel = scaler.fit(allPointsDF)
    scalerModel.transform(allPointsDF)
  }

  def transformToTraining(allPoints: RDD[(Double, Vector)],
                          sqlContext: SQLContext) = {

    scaleFeatures(allPoints, sqlContext, "features")
  }

  // TODO - use a pipeline instead here
  def transformToTrainingForPoly(polyDegree: Int)(allPoints: RDD[(Double, Vector)], sqlContext: SQLContext) = {
    val scaled = scaleFeatures(allPoints, sqlContext, "scaledFeatures")

    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("scaledFeatures")
      .setOutputCol("features")
      .setDegree(polyDegree)
      .transform(scaled)
    polynomialExpansion
  }

  //def trainModelsWithVaryingPoly(seed : Long = 11L) = {
    //val allPoints = deriveAllPointsFromLabeledFreqs(sc).cache()
    //val logName = "log/training-poly-" + System.currentTimeMillis()

      //val somePoints = sc.parallelize( allPoints.takeSample(false, 250, seed) )
      //somePoints.cache()

    //val poly = if(devEnv) (2 to 2) else (2 to 8)
    //for(p <- poly) {
      //trainOfflineModel(somePoints,transformToTrainingForPoly(p), 25, "")
    //}
  //}

  val logName = "log/training-" + System.currentTimeMillis()
  val byTenPercentIncrements = (.10 to 1 by .10)
  def trainModelsWithVaryingM(seed : Long = 11L) = {
    def calcM(allPointsCount : Long) : scala.collection.immutable.Range = {
      val tenPer = (allPointsCount * 0.10).toInt
      (tenPer to allPointsCount.toInt by tenPer)
    }
    // create an RDD of training points
    val allPoints = deriveAllPointsFromLabeledFreqs(sc).cache()
    val logName = "log/training-" + System.currentTimeMillis()

    val allPointsCount = allPoints.count()

    println("all points: " + allPointsCount)
    val transformedData = transformToTraining(allPoints, sqlContext).cache()

    val rangeAndStep = if(devEnv) (100 to 100 by 100) else calcM(allPointsCount)
    for(m <- byTenPercentIncrements) {
      val somePoints = transformedData.sample(false, m, seed)
      trainOfflineModel(somePoints, transformToTraining, logName)
    }
  }

  //def trainModelWithAllData() = {
    //val allPoints = deriveAllPointsFromLabeledFreqs(sc).cache()
    //val logName = "log/training-" + System.currentTimeMillis()

    //println("total training samples: " + allPoints.count())
    //trainOfflineModel(allPoints, transformToTraining, 10, logName)
  //}

  def trainOfflineModel(allPoints: DataFrame,
                        transformationFx : (RDD[(Double, Vector)], SQLContext) => DataFrame,
                        logName : String = "") = {

    val splits = allPoints.randomSplit(Array(0.8, 0.1, 0.1), seed = 11L)
    val training = splits(0).cache()
    val crossVal = splits(1).cache()
    val test = splits(2).cache()

    val regRange = if(devEnv) (1 to 5) else (1 to 30)
    val lotsofRegParams = regRange.foldLeft(Array[Double](10)) { (acc, n) => acc :+ (acc.last/1.5) }

    val lr = new LogisticRegression()
      .setMaxIter(3000)
      .setFeaturesCol("features")

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, lotsofRegParams)
      .addGrid(lr.elasticNetParam, Array(0.0))
      .build()

    paramGrid.foreach { modelParam => runTrainingAndCV(lr, modelParam, training, crossVal) }
  }

  def runTrainingAndCV(lr : LogisticRegression, modelParams : ParamMap, trainingData : DataFrame, cvData : DataFrame) = {
      //val evaluator = new BinaryClassificationEvaluator()
      //.setMetricName("areaUnderPR")
      val evaluator = new MulticlassClassificationEvaluator()

      val model = lr.fit(trainingData, modelParams)
      val trainingEval = evaluator.evaluate(
        model.transform(trainingData, modelParams))
      val cvEval = evaluator.evaluate(
        model.transform(cvData, modelParams))
      printModelMetrics(trainingData.count, trainingEval, cvEval, model, logName)
  }

  def trainOfflineModelAG(allPoints: DataFrame,
                        transformationFx : (RDD[(Double, Vector)], SQLContext) => DataFrame,
                        m : Int = 100, // TODO: can this be removed
                        logName : String = "") = {
    //allPoints.saveAsTextFile("hdfs://spark3.thedevranch.net/football/allTrainingPoints")
    // read previously created points

    //val transformedData = transformationFx(allPoints, sqlContext)


    val splits = allPoints.randomSplit(Array(0.8, 0.1, 0.1), seed = 11L)
    val training = splits(0).cache()
    val crossVal = splits(1).cache()
    val test = splits(2).cache()

    // header:
    //println("regParam, iterations, f1, precision, recall")

    val lr = new LogisticRegression()
      .setMaxIter(3000)
      .setFeaturesCol("features")

    val regRange = if(devEnv) (1 to 5) else (1 to 30)
    val lotsofRegParams = regRange.foldLeft(Array[Double](10)) { (acc, n) => acc :+ (acc.last/1.5) }
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, lotsofRegParams)
      .addGrid(lr.elasticNetParam, Array(0.0))
      .build()

    //val evaluator = new BinaryClassificationEvaluator()
    //.setMetricName("areaUnderPR")

    //val trainValidationSplit = new TrainValidationSplit()
      //.setEstimator(lr)
      //.setEvaluator(evaluator)
      //.setEstimatorParamMaps(paramGrid)
      //.setTrainRatio(0.8)


    val evaluator = new MulticlassClassificationEvaluator()
    paramGrid.foreach { modelParams =>
      val model = lr.fit(training, modelParams)
      val trainingEval = evaluator.evaluate(
        model.transform(training, modelParams))
      val cvEval = evaluator.evaluate(
        model.transform(crossVal, modelParams))
      printModelMetrics(training.count, trainingEval, cvEval, model, logName)

    }
    //val savableModel = lr.fit(training, model.bestModel.extractParamMap)
    //val metric = evaluator.evaluate(model.bestModel.transform(training, model.bestModel.extractParamMap))
    //println(s"metric : $metric")

  }

  def printModelMetrics(m : Long, trainingEval : Double, cvEval : Double, model : LogisticRegressionModel, appendFileName : String = "") = {
    //model.bestModel.summary.objectiveHistory.foreach(println)
    val first = Array[String](m.toString,
                              trainingEval.toString,
                              cvEval.toString,
                              model.summary.objectiveHistory.length.toString)

    // TODO: just get this from the paramGrid later
    val interestingParams = Array[String]("elasticNetParam", "regParam")
    val params = model.extractParamMap().toSeq.map {
      case pp : ParamPair[_] => (pp.param.name, pp.value)
    }.filter( interestingParams contains _._1 ).sortBy(_._1)
    .foldLeft(first) { (acc, n) => acc :+ n._1 + "," + n._2 }

    val strToWrite = params.mkString(",")
    if(appendFileName != "") scala.tools.nsc.io.File(appendFileName).appendAll(strToWrite + "\n")
    println(strToWrite)
  }

  def modelEvaluation(model : TrainValidationSplitModel, testSet : DataFrame) = {

    val predictionAndLabel = model.transform(testSet)
      .select("label", "rawPrediction", "prediction")
      .map {
        case Row(label: Double, rawPrediction: Vector, prediction: Double) => (label, prediction)
      }
      val metrics = new MulticlassMetrics(predictionAndLabel)
      //println(modelParam.regParam + "," + modelParam.numIterations + "," + metrics.fMeasure + "," + metrics.precision + "," + metrics.recall)
      //}

      //println( model.toPMML() )
  }
  def persistModel(model : TrainValidationSplitModel) = {
      //val savableModel = lr.fit(training, model.bestModel.extractParamMap)
      //val modelSaveString = "models/logreg.model-" + System.currentTimeMillis()
      //savableModel.save(modelSaveString)
  }
}
