package muteButton

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {

  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("muteButton")
    //val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(2))

    Logger.getRootLogger().setLevel(Level.ERROR)

    //val lines = sc.textFile("/media/brycemcd/filestore/spark2bkp/football/freqs-60s.txt")
    val lines = ssc.socketTextStream("localhost", 9999)
    val meanByKey = FrequencyIntensity.convertFileContentsToMeanIntensities(lines)

    println("====")
    meanByKey.print()
    println("====")

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
    //sc.stop()
  }
}
