package muteButton.Test

import org.scalatest._
import muteButton.SummaryStats
import org.scalameter._

import breeze.linalg._
import breeze.numerics._

object BenchMark extends Tag("BenchMark")
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

  describe("#normalizedArray") {
    val normalized = new SummaryStats(testArr).normalizedArray
    val normalizedSolution = Array(-1.414213562373095, -0.7071067811865475, 0.0, 0.7071067811865475, 1.414213562373095)
    it("returns an array of normalized values") {
      for(i <- 0 until testArr.length) {
        assert(normalized(i) == normalizedSolution(i))
      }
      assert(normalized.length == normalizedSolution.length)
    }
  }

  describe("#cntMoreThanN") {
    it("returns a count of the number of items in the array that are > N") {
      val arrWithNegs = Array(-4.0, -5.0, -1.0, 1.0, 2.0, 3.0, 4.0, 5.0)
      val cnt = new SummaryStats(arrWithNegs).cntMoreThanN(3.0)
      assert(cnt  == 4)
    }
  }

  describe("benchmarking") {
    it("tests time", BenchMark) {

      val time = measure {
        for(i <- 0 until 10) {
          new SummaryStats(DenseVector.rand(2048).toArray).normalizedArray
        }
      }
      println(s"lrmTime: $time")
    }
  }
}
