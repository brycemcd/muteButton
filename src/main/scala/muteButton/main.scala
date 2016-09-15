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
//import sqlContext.implicits._
import org.apache.spark.ml.feature.StandardScaler

import org.apache.log4j.Logger
import org.apache.log4j.Level

// spark.ml
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType};


// NOTE: I just lifted this out of ml-playground. Remove it and just use
// the playground later. I really should be using it here but I'm on a roll
case class SGDModelParams( regParam: Double,
                           numIterations: Int) {

}

object Main {

  lazy val conf = new SparkConf()
    .setAppName("muteButton")
    .setMaster("local[*]")
    .set("spark.executor.memory", "10g")
    .set("spark.executor-memory", "30g")
    .set("spark.driver.memory", "10g")
    .set("spark.driver-memory", "10g")
  lazy val sc = new SparkContext(conf)
  val streamWindow = 2
  lazy val ssc = new StreamingContext(sc, Seconds(streamWindow))
  val numberOfFrequenciesCaptured = 1024
  val frequenciesInWindow = numberOfFrequenciesCaptured * streamWindow


  def main(args: Array[String]) = {
    sc // init it here to quiet the logs and make stopping easier
    Logger.getRootLogger().setLevel(Level.ERROR)
    //protectSanity
    trainOfflineModel()
    //predictFromStream( PredictionAction.negativeCase,
                       //PredictionAction.positiveCase)
    //getFreqs()
  }
  private def generateModelParams : Seq[SGDModelParams] = {
    // NOTE: I couldn't find in my notes if these were sensible defaults
    val regs = Seq[Double](0.01, 0.1)
    for(regParam <- regs;
         numIterations <- (3000 to 3000 by 10) ) yield SGDModelParams(regParam, numIterations)
  }

  def trainOfflineModel() = {
    // Loads data.
    // NOTE that freq is a somewhat "magic" (now conventional ;) ) prepend string
    // for training prepared data
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
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

    val allPoints = trainGamePoints.union(trainAdPoints)
    val allPointsDF = sqlContext.createDataFrame(allPoints)
                                  .toDF("label", "rawfeatures")

    val scaler = new StandardScaler()
      .setInputCol("rawfeatures")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(false)

    val scalerModel = scaler.fit(allPointsDF)
    val transformedData = scalerModel.transform(allPointsDF)

    val splits = transformedData.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1).cache()

    // header:
    println("regParam, iterations, f1, precision, recall")
    generateModelParams.map { modelParam =>
      //println(s"fitting new model with $modelParam")

      // TODO: this is the new one
      var lr = new LogisticRegression()
        .setMaxIter(modelParam.numIterations)
        .setRegParam(modelParam.regParam)
        .setElasticNetParam(0.8)
        .setFeaturesCol("features")

      var model = lr.fit(training)


      var trainingSummary = model.summary
      var objectiveHistory = trainingSummary.objectiveHistory
      objectiveHistory.foreach(loss => println(loss))

      var predictionAndLabel = model.transform(test)
        .select("label", "rawPrediction", "prediction")
        .map {
          case Row(label: Double, rawPrediction: Vector, prediction: Double) => (label, prediction)
        }
      var metrics = new MulticlassMetrics(predictionAndLabel)
      println(modelParam.regParam + "," + modelParam.numIterations + "," + metrics.fMeasure + "," + metrics.precision + "," + metrics.recall)
    }

    //println("training all points " + allPoints.count())


    //val modelSaveString = "models/kmeans.model-" + System.currentTimeMillis()
    //model.save(sc, modelSaveString)

    //println( model.toPMML() )
    sc.stop()
  }

  def protectSanity = {
    val lines = ssc.socketTextStream("10.1.2.230", 9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()  // Wait for the computation to terminate
    sc.stop()
  }

  def predictFromStream(negativeAction : () => Int,
                        positiveAction : () => Int) = {
    //val modelPath = "models/kmeans.model-1473003588743"
    //val model = KMeansModel.load(sc, modelPath)

    //val lines = ssc.socketTextStream("10.1.2.230", 9999)
    //Logger.getRootLogger().setLevel(Level.ERROR)

    //val meanByKey = FrequencyIntensityStream.convertFileContentsToMeanIntensities(lines)
    //meanByKey.foreachRDD { mbk =>
      //val vec = Vectors.dense( mbk.map(_._2).take( frequenciesInWindow ) )
      //if(vec.size == frequenciesInWindow) {
        //val prediction = model.predict(vec)
        //println("prediction: ---- " + prediction + " ----")
        //if(prediction == 0) negativeAction() else positiveAction()
      //} else {
        //print("not 2048: ")
        //print(vec.size)
      //}
      //println()
    //}

    //ssc.start()
    //ssc.awaitTermination()  // Wait for the computation to terminate
    //sc.stop()
  }
}
