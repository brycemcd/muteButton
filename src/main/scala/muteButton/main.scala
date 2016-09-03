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

  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("muteButton")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    Logger.getRootLogger().setLevel(Level.ERROR)
    // Loads data.
    val lines = sc.textFile("/media/brycemcd/filestore/spark2bkp/football/freqs-60s.txt")
    val meanByKey = FrequencyIntensity.convertFileContentsToMeanIntensities(lines)

    println("====")
    meanByKey.map( _._2 ).take(16).map(println)
    println("====")
    //val kmeans = new KMeans().setK(2).setSeed(1L).setFeaturesCol("features")
    //import sqlContext.implicits._

    //case class Freq(freq: String, inten: Double)
    //val df = meanByKey.map( x => Freq(x._1.toString(), x._2) ).toDF()
    //df.registerTempTable("df")
    //val model = kmeans.fit(df)
    //
    // take 2
    val numClusters = 2
    val numIterations = 20
    val parsedData = meanByKey.map( freq => Vectors.dense( freq._2 ) )
    val model = KMeans.train(parsedData, numClusters, numIterations)

    println(model.toPMML())
    println("====")

    // Trains a k-means model.

    sc.stop()
  }
}
