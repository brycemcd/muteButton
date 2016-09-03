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

  def main(args: Array[String]) = {
    Logger.getRootLogger().setLevel(Level.ERROR)
    predictFromModel()
  }

  def mainStream(args: Array[String]) = {
    //val conf = new SparkConf().setMaster("local[2]").setAppName("muteButton")
    ////val sc = new SparkContext(conf)
    //val ssc = new StreamingContext(conf, Seconds(2))

    //Logger.getRootLogger().setLevel(Level.ERROR)

    ////val lines = sc.textFile("/media/brycemcd/filestore/spark2bkp/football/freqs-60s.txt")
    //val lines = ssc.socketTextStream("localhost", 9999)
    //val meanByKey = FrequencyIntensity.convertFileContentsToMeanIntensities(lines)

    //println("====")
    //meanByKey.transform(_.toDF())
    //println("====")

    //ssc.start()             // Start the computation
    //ssc.awaitTermination()  // Wait for the computation to terminate
  }


  lazy val conf = new SparkConf().setMaster("local[2]").setAppName("muteButton")
  lazy val sc = new SparkContext(conf)
  def trainModel() = {
    // Loads data.
    val lines = sc.textFile("/media/brycemcd/filestore/spark2bkp/football/freqs-60s.txt")
    val meanByKey = FrequencyIntensity.convertFileContentsToMeanIntensities(lines)

    // Trains a k-means model.
    val numClusters = 2
    val numIterations = 20
    val parsedData = meanByKey.map( freq => Vectors.dense( freq._2 ) )
    val model = KMeans.train(parsedData, numClusters, numIterations)

    val modelSaveString = "models/kmeans.model-" + System.currentTimeMillis()
    model.save(sc, modelSaveString)

    println(model.toPMML())
    println("====")

    sc.stop()
  }

  def predictFromModel() = {
    val modelPath = "models/kmeans.model-1472921561028"
    val model = KMeansModel.load(sc, modelPath)

    val lines = sc.textFile("/media/brycemcd/filestore/spark2bkp/football/freqs-5s.txt")
    val meanByKey = FrequencyIntensity.convertFileContentsToMeanIntensities(lines)
    meanByKey.foreach { mbk =>
      val vec = Vectors.dense( mbk._2 )
      println("prediction: ----")
      println( model.predict(vec) )
      println("----")
    }

    sc.stop()
  }
}
