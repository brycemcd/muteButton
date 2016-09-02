package muteButton

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {

  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("muteButton")
    val sc = new SparkContext(conf)
    Logger.getRootLogger().setLevel(Level.ERROR)

    val lines = sc.textFile("/media/brycemcd/filestore/spark2bkp/football/freqs-60s.txt")
    val meanByKey = FrequencyIntensity.convertFileContentsToMeanIntensities(lines)

    println("====")
    meanByKey.take(15).map(println)
    println("====")

    sc.stop()
  }
}
