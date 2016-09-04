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

  lazy val conf = new SparkConf().setMaster("local[2]").setAppName("muteButton")
  lazy val sc = new SparkContext(conf)
  lazy val ssc = new StreamingContext(sc, Seconds(2))

  def main(args: Array[String]) = {
    sc // init it here to quiet the logs and make stopping easier
    Logger.getRootLogger().setLevel(Level.ERROR)
    protectSanity
    //trainOfflineModel()
    //predictFromStream()
    //getFreqs()
    sc.stop()
  }

  def getFreqs() = {
    val lines = sc.textFile("/media/brycemcd/filestore/spark2bkp/football/freqs/freqs-60s.txt")
    val meanByKey = FrequencyIntensityRDD.convertFileContentsToMeanIntensities(lines)
    println("====")
    meanByKey.take(10).map(println)
    println("====")
  }

  def trainOfflineModel() = {
    // Loads data.
    val lines = sc.textFile("/media/brycemcd/filestore/spark2bkp/football/freqs/ad_and_game.txt")
    val meanByKey = FrequencyIntensityRDD.convertFileContentsToMeanIntensities(lines)

    // NOTE: The way I have sox piping data in streams 2048 lines per cycle
    // if that changes, this needs to change
    val magicFreqCountNumber = 2048

    // Trains a k-means model.
    val groupedFrequencies = meanByKey.collect().grouped(magicFreqCountNumber).map { g =>
      Vectors.dense( g.map(_._2) )
    }
    val groupedFrequenciesRDD = sc.parallelize( groupedFrequencies.toList )

    val numClusters = 2
    val numIterations = 20
    //val parsedData = meanByKey.map( freq => Vectors.dense( freq._2 ) )
    val model = KMeans.train(groupedFrequenciesRDD, numClusters, numIterations)

    val modelSaveString = "models/kmeans.model-" + System.currentTimeMillis()
    model.save(sc, modelSaveString)

    println(model.toPMML())
    println("====")

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
  def predictFromStream() = {
    val modelPath = "models/kmeans.model-1473003928958"
    val model = KMeansModel.load(sc, modelPath)

    val lines = ssc.socketTextStream("10.1.2.230", 9999)
    Logger.getRootLogger().setLevel(Level.ERROR)

    val meanByKey = FrequencyIntensityStream.convertFileContentsToMeanIntensities(lines)
    //meanByKey.foreachRDD { mbk =>
      ////println("===")
      //val vec = Vectors.dense( mbk.map(_._2).take( 2048 ) )
      //////println(vec)
      //print("prediction: ---- ")
      //print( model.predict(vec) )
      //print(" ----")
      //println()
      //println("===")
    //}

    meanByKey.print()
    ssc.start()
    ssc.awaitTermination()  // Wait for the computation to terminate
    sc.stop()
  }

  def predictFromFile() = {
    //val modelPath = "models/kmeans.model-1472921561028"
    //val model = KMeansModel.load(sc, modelPath)

    ////val lines = sc.textFile("/media/brycemcd/filestore/spark2bkp/football/freqs-5s.txt")
    //val lines = ssc.socketTextStream("localhost", 9999)
    //val meanByKey = FrequencyIntensityStream.convertFileContentsToMeanIntensities(lines)
    //meanByKey.foreach { mbk =>
      //val vec = Vectors.dense( mbk._2 )
      //println("prediction: ----")
      //println( model.predict(vec) )
      //println("----")
    //}

    //ssc.start()             // Start the computation
    //ssc.awaitTermination()  // Wait for the computation to terminate
  }

  def testStream(args: Array[String]) = {
    //val meanByKey = FrequencyIntensity.convertFileContentsToMeanIntensities(lines)

    //println("====")
    //meanByKey.transform(_.toDF())
    //println("====")

    //ssc.start()             // Start the computation
    //ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
