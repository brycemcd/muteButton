package muteButton

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.param.ParamPair

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
  sc: SparkContext
) {

  lazy private val sqlContext = new SQLContext(sc)

  private def deriveAllPointsFromLabeledFreqs(sc : SparkContext) : RDD[(Double, Vector)] = {
    //val trainAdLines = sc.textFile("/media/brycemcd/filestore/spark2bkp/football/supervised_samples/ad/freqs/ari_phi_chunked091_freqs-labeled.txt")
    //val trainGameLines = sc.textFile("/media/brycemcd/filestore/spark2bkp/football/supervised_samples/game/freqs/ari_phi_chunked095_freqs-labeled.txt")
    val trainAdLines = sc.textFile("/media/brycemcd/filestore/spark2bkp/football/supervised_samples/ad/freqs/*-labeled.txt")
    val trainGameLines = sc.textFile("/media/brycemcd/filestore/spark2bkp/football/supervised_samples/game/freqs/*-labeled.txt")

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

  def transformToTraining(allPoints: RDD[(Double, Vector)], sqlContext: SQLContext) = {

    val allPointsDF = sqlContext.createDataFrame(allPoints)
      .toDF("label", "rawfeatures")

    val scaler = new StandardScaler()
      .setInputCol("rawfeatures")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(false)

    val scalerModel = scaler.fit(allPointsDF)
    scalerModel.transform(allPointsDF)
  }

  def trainModelsWithVaryingM(seed : Long = 11L) = {
    // create an RDD of training points
    val allPoints = deriveAllPointsFromLabeledFreqs(sc).cache()
    val logName = "log/training-" + System.currentTimeMillis()
    for(m <- (100 to 10000 by 200)) {
      val somePoints = sc.parallelize( allPoints.takeSample(false, m, seed) )
      trainOfflineModel(somePoints, m, logName)
    }
  }

  def trainOfflineModel(allPoints: RDD[(Double, Vector)], m : Int = 100, logName : String = "") = {
    //allPoints.saveAsTextFile("hdfs://spark3.thedevranch.net/football/allTrainingPoints")
    // read previously created points

    val transformedData = transformToTraining(allPoints, sqlContext)

    val splits = transformedData.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1).cache()

    // header:
    //println("regParam, iterations, f1, precision, recall")

    val lr = new LogisticRegression()
      .setMaxIter(3000)
      .setFeaturesCol("features")

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01, 0.01))
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    //val evaluator = new BinaryClassificationEvaluator()
    //.setMetricName("areaUnderPR")
    val evaluator = new MulticlassClassificationEvaluator()

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)


    val model = trainValidationSplit.fit(training)
    printModelMetrics(training.count, model, logName)
    val savableModel = lr.fit(training, model.bestModel.extractParamMap)
    val metric = evaluator.evaluate(model.bestModel.transform(training, model.bestModel.extractParamMap))
    println(s"metric : $metric")

    val predictionAndLabel = model.transform(test)
      .select("label", "rawPrediction", "prediction")
      .map {
        case Row(label: Double, rawPrediction: Vector, prediction: Double) => (label, prediction)
      }
      val metrics = new MulticlassMetrics(predictionAndLabel)
      //println(modelParam.regParam + "," + modelParam.numIterations + "," + metrics.fMeasure + "," + metrics.precision + "," + metrics.recall)
      //}

      //println( model.toPMML() )
  }

  def printModelMetrics(m : Long, model : TrainValidationSplitModel, appendFileName : String = "") = {
    //model.bestModel.summary.objectiveHistory.foreach(println)
    for(i <- (0 to model.getEstimatorParamMaps.length - 1)) {
      val first = Array[String](m.toString, model.validationMetrics(i).toString)
      val params = model.getEstimatorParamMaps(i).toSeq.map {
        case pp : ParamPair[_] => (pp.param.name, pp.value)
      }.foldLeft(first) { (acc, n) => acc :+ n._1 + "," + n._2 }

      val strToWrite = params.mkString(",")
      if(appendFileName != "") scala.tools.nsc.io.File(appendFileName).appendAll(strToWrite + "\n")
      println(strToWrite)
    }
  }

  def persistModel(model : TrainValidationSplitModel) = {
      //val savableModel = lr.fit(training, model.bestModel.extractParamMap)
      //val modelSaveString = "models/logreg.model-" + System.currentTimeMillis()
      //savableModel.save(modelSaveString)
  }
}
