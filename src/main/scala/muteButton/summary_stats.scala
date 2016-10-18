package muteButton

object SummaryStats {

}

class SummaryStats(val dataSet: Array[Double]) {

  def mean = dataSet.sum / dataSet.length

  def variance = {
    // PERF: stop recalculating all the things!
    val preMean = mean

    dataSet.foldLeft(0.0) { (acc, v) =>
      scala.math.pow( (v - preMean), 2.0) + acc
    } * 1/dataSet.length
  }

  def sd = scala.math.sqrt(variance)

  def range = dataSet.max - dataSet.min

  def cntMoreThanN(n : Double) : Int = {
    dataSet.foldLeft(0) { (acc, v) =>
      v match {
        case _ if math.abs(v) > n => acc + 1
        case _ => acc
      }
    }
  }

  def normalizedArray = {
    // PERF: stop recalculating all the things!
    val preSD = sd
    val preMean = mean

    dataSet.foldLeft(List[Double]()) { (acc, v) =>
        ((v - preMean) / preSD) +: acc
    }.reverse.toArray
  }
}
