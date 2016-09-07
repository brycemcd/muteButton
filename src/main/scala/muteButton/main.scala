package muteButton

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.Vectors


import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {

  lazy val conf = new SparkConf().setMaster("local[*]").setAppName("muteButton")
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

  def trainOfflineModel() = {
    // Loads data.
    // NOTE that freq is a somewhat "magic" (now conventional ;) ) prepend string
    // for training prepared data
    val lines = sc.textFile("/media/brycemcd/filestore/spark2bkp/football/freqs/exp/freqzz*")
    val meanByKey = FrequencyIntensityRDD.convertFileContentsToMeanIntensities(lines)

    // NOTE: I should start with a collection of (filename, (freq, intensity)) tuples
    // NOTE: There's a slight risk that frequencies and intensity examples will
    // get mixed up when this is distributed across a cluster
    val groupedFrequenciesRDD = meanByKey.groupByKey().map(_._2).map(_.toArray.sortBy(_._1)).map { x =>
      Vectors.dense( x.map(_._2) )
    }

    // NOTE: this triggers an action, consider if this is useful
    println("training on " + groupedFrequenciesRDD.count() + " cases")

    val numClusters = 2
    val numIterations = 20
    val model = KMeans.train(groupedFrequenciesRDD, numClusters, numIterations)
    val modelSaveString = "models/kmeans.model-" + System.currentTimeMillis()
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

  def predictFromStream(negativeAction : () => Unit,
                        positiveAction : () => Unit) = {
    val modelPath = "models/kmeans.model-1473003928958"
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
