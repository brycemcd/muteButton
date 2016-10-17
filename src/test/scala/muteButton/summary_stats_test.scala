package muteButton.Test

import org.scalatest._
import muteButton.SummaryStats
class SummaryStatsTest extends FunSpec {
  val testArr = Array(1.0, 2.0, 3.0, 4.0, 5.0)

  describe("#mean") {
    it("calculates the mean of the dataset") {
      val mean = new SummaryStats(testArr).mean
      assert(mean == 3.0)
    }
  }

  describe("#variance") {
    it("calculates variance correctly") {
      val variance = new SummaryStats(testArr).variance
      assert(variance  == 2.0)
    }
  }

  describe("#sd") {
    it("calculates standard deviation") {
      val sd = new SummaryStats(testArr).sd
      assert(sd  == scala.math.sqrt(2.0))
    }
  }

  describe("#range") {
    it("calculates range of the data") {
      val range = new SummaryStats(testArr).range
      assert(range  == 4.0)
    }
  }
}
