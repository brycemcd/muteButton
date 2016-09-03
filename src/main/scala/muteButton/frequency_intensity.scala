package muteButton

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming._

import org.apache.log4j.Logger
import org.apache.log4j.Level

object FrequencyIntensity {
  type FreqIntensities = DStream[(Double, (Double, Int))]

  def mapFileToFreqIntensityList(fileContents : ReceiverInputDStream[String]) : FreqIntensities = {
    val addToList = (li: List[Double], d: Double) => d +: li
    val sumLists = (p1: List[Double], p2: List[Double]) => p1 ::: p2

    fileContents.map(_.split("  ")).filter(_.length == 2).map {
      case tup : Array[String] => (tup(0).toDouble, (tup(1).toDouble, 1))
    }.reduceByKey {
      (a : (Double, Int), v : (Double, Int)) => (a._1 + v._1, a._2 + v._2)
    }
  }

  def meanFrequencyIntensities(freqTuple : FreqIntensities) = {
    freqTuple.map {
      case (freq, (sum, count)) => (freq, sum * 1/count)
    }.transform(_.sortByKey(true))
  }

  def convertFileContentsToMeanIntensities(fileContents : ReceiverInputDStream[String]) = {
    meanFrequencyIntensities( mapFileToFreqIntensityList(fileContents))
  }
}
