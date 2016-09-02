package muteButton

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

object FrequencyIntensity {
  type FreqIntensities = RDD[(Double, List[Double])]

  def mapFileToFreqIntensityList(fileContents : RDD[String]) : FreqIntensities  = {
    val addToList = (li: List[Double], d: Double) => d +: li
    val sumLists = (p1: List[Double], p2: List[Double]) => p1 ::: p2

    fileContents.map(_.split("  ")).filter(_.length == 2).map {
      case tup : Array[String] => (tup(0).toDouble, tup(1).toDouble)
    }.aggregateByKey(List[Double]())(addToList, sumLists)
  }

  def meanFrequencyIntensities(freqs : FreqIntensities) : RDD[(Double, Double)] = {
    val mean = (li: List[Double]) => li.sum / li.length
    freqs.map( tup => (tup._1, mean(tup._2))).sortByKey()
  }

  def convertFileContentsToMeanIntensities(fileContents : RDD[String]) : RDD[(Double, Double)] = {
    meanFrequencyIntensities( mapFileToFreqIntensityList(fileContents))
  }
}
