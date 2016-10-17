package muteButton

object SummaryStats {

}

class SummaryStats(val dataSet: Array[Double]) {

  def mean = dataSet.sum / dataSet.length

  def variance = {
    dataSet.foldLeft(0.0) { (acc, v) =>
      scala.math.pow( (v-mean), 2.0) + acc
    } * 1/dataSet.length
  }

  def sd = scala.math.sqrt(variance)

  def range = dataSet.max - dataSet.min
}
