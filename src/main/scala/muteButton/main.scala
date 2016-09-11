package muteButton

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD,LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}

import org.apache.spark.sql.SQLContext
import sqlContext.implicits._

import org.apache.log4j.Logger
import org.apache.log4j.Level

// NOTE: I just lifted this out of ml-playground. Remove it and just use
// the playground later. I really should be using it here but I'm on a roll
case class SGDModelParams( regParam: Double,
                           numIterations: Int) {

}

object Main {

  lazy val conf = new SparkConf()
    .setAppName("muteButton")
    .setMaster("local[*]")
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
    val regs = Seq[Double](0.001, 0.01, 0.1, 1, 3, 10)
    for(regParam <- regs;
         numIterations <- (5 to 10 by 10) ) yield SGDModelParams(regParam, numIterations)
  }

  def trainOfflineModel() = {
    // Loads data.
    // NOTE that freq is a somewhat "magic" (now conventional ;) ) prepend string
    // for training prepared data
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
      new LabeledPoint(1.0, Vectors.dense( x.map(_._2) ))
    }

    val trainGamePoints = trainGameTouples.groupByKey().map(_._2).map(_.toArray.sortBy(_._1)).map { x =>
      new LabeledPoint(0.0, Vectors.dense( x.map(_._2) ))
    }


    //println("training ad points " + trainAdPoints.count())
    //println("training game points " + trainGamePoints.count())

    val allPoints = trainGamePoints.union(trainAdPoints).toDF()

    val splits = allPoints.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1).cache()

    // header:
    println("regParam, iterations, f1, precision, recall")
    generateModelParams.map { modelParam =>
      //val fit = new LogisticRegressionWithSGD()
      //fit.optimizer.
        //setNumIterations(modelParam.numIterations).
        //setRegParam(modelParam.regParam)
      //val model = fit.run(training)
        val fit = new LogisticRegression()
          .setMaxIter(modelParam.numIterations)
          .setRegParam(modelParam.regParam)
          .setElasticNetParam(0.8)
        val model = fit.fit(training)


      //model.clearThreshold()
      val trainingSummary = model.summary
      val objectiveHistory = trainingSummary.objectiveHistory
      objectiveHistory.foreach(loss => println(loss))


      val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
      }

      val metrics = new MulticlassMetrics(predictionAndLabels)
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
    val modelPath = "models/kmeans.model-1473003588743"
    val model = KMeansModel.load(sc, modelPath)

    val lines = ssc.socketTextStream("10.1.2.230", 9999)
    Logger.getRootLogger().setLevel(Level.ERROR)

    val meanByKey = FrequencyIntensityStream.convertFileContentsToMeanIntensities(lines)
    meanByKey.foreachRDD { mbk =>
      val vec = Vectors.dense( mbk.map(_._2).take( frequenciesInWindow ) )
      if(vec.size == frequenciesInWindow) {
        val prediction = model.predict(vec)
        println("prediction: ---- " + prediction + " ----")
        if(prediction == 0) negativeAction() else positiveAction()
      } else {
        print("not 2048: ")
        print(vec.size)
      }
      println()
    }

    ssc.start()
    ssc.awaitTermination()  // Wait for the computation to terminate
    sc.stop()
  }
}
