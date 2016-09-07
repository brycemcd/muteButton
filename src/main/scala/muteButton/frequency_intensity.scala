package muteButton

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming._

import org.apache.log4j.Logger
import org.apache.log4j.Level

// A = DStream, B = ReceiverInputDStream for streaming things
// A = RDD, B =    for reading files
package object NewTypes {
  type FreqIntensities = (Double, (Double, Int))
}

import muteButton.NewTypes._
import scala.util.matching.Regex

trait FrequencyIntensity [
  A,
  B,
  C
] {
  def mapFileToFreqIntensityList(fileContents : A) : B



  def meanFrequencyIntensities(freqTuple : B) : C


}

object FrequencyIntensityRDD extends FrequencyIntensity[
  RDD[String],
  RDD[FreqIntensities],
  RDD[(Double, Double)]
] {
  def mapFileToFreqTrainingIntensity(fileContents : RDD[String]) = {
    val freqIntensLines = """(\d{1,}\.\d{1,})  (\d{1,}\.\d{1,})  (freq.*)""".r

    fileContents.flatMap {
        case freqIntensLines(freq, intense, seqNum) => Some( (seqNum, (freq.toDouble, intense.toDouble)) )
        case _ => None
    }
  }

  def mapFileToFreqIntensityList(fileContents : RDD[String]) = {
    fileContents.map(_.split("  ")).filter(_.length == 2).map {
      case tup : Array[String] => (tup(0).toDouble, (tup(1).toDouble, 1))
    }.reduceByKey {
      (a : (Double, Int), v : (Double, Int)) => (a._1 + v._1, a._2 + v._2)
    }
  }

  def meanFrequencyIntensities(freqTuple : RDD[FreqIntensities]) : RDD[(Double, Double)] = {
    freqTuple.map {
      case (freq : Double, (sum : Double, count : Int)) => (freq, sum * 1/count)
    }.sortByKey(true)
  }

  //def convertFileContentsToMeanIntensities(fileContents : RDD[String]) : RDD[(Double, Double)] = {
  def convertFileContentsToMeanIntensities(fileContents : RDD[String]) = {
    //meanFrequencyIntensities(
      //mapFileToFreqIntensityList(fileContents)
    //)
    mapFileToFreqTrainingIntensity(fileContents)
  }
}

object FrequencyIntensityStream {
  def mapFileToFreqIntensityList(fileContents : DStream[String]) = {
    fileContents.map(_.split("  ")).filter(_.length == 2).map {
      case tup : Array[String] => (tup(0).toDouble, (tup(1).toDouble, 1))
    }.reduceByKey {
      (a : (Double, Int), v : (Double, Int)) => (a._1 + v._1, a._2 + v._2)
    }
  }

  // works for streaming
  def meanFrequencyIntensities(freqTuple : DStream[FreqIntensities]) = {
    freqTuple.map {
      case (freq, (sum, count)) => (freq, sum * 1/count)
    }.transform(_.sortByKey(true))
  }

  def convertFileContentsToMeanIntensities(fileContents : DStream[String]) = {
    meanFrequencyIntensities(
      mapFileToFreqIntensityList(fileContents)
    )
  }
}
