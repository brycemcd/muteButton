package muteButton.Test

import org.scalatest._
import muteButton.FrequencyIntensityStreamWithList
import scala.io.Source._

class FrequencyIntensityTest extends FunSpec {
  // NOTE: this is somewhat of a "magic" file. It's a reference for what
  // will be delivered during training and streamed during predictions
  val freqFile = fromFile("src/test/resources/car_den_20160908_2_chunked067_freqs.txt").getLines
  val freqFileGroupCount = 117
  val freqIntenseCount   = 2048

  describe("stream conversion") {
    describe(".parseLine") {
      val goodLine = """0.000000  1.215537"""
      val goodLineTuple = Some(Tuple2(0.000000, 1.215537))
      val bunkLine = """This is [some worthless ===-]"""
      describe("clean line") {
        it("converts a long file of freq-intense touples to a List of Lists") {
          val result = FrequencyIntensityStreamWithList.parseLine(goodLine)
          assert(result.nonEmpty)
          assert(result == goodLineTuple)
        }
      }

      describe("totally worthless line") {
        it("returns None") {
          val result = FrequencyIntensityStreamWithList.parseLine(bunkLine)
          assert(result == None)
        }
      }

      describe("line with freq-intense and garbage") {
        val mixedLine = bunkLine + "     " + goodLine
        val result = FrequencyIntensityStreamWithList.parseLine(mixedLine)
        it("returns just the freq-intense") {
          assert(result.nonEmpty)
          assert(result == goodLineTuple)
        }
      }
    }

    describe(".isFirstLine") {
      describe("when freq is 0.00") {
        val testTup = (0.00, 1.234)
        it("is a first line") {
          assert(FrequencyIntensityStreamWithList.isFirstLine(testTup) == true)
        }
      }

      describe("anything else") {
        val testTup = (1.234, 2.345)
        it("is NOT a first line") {
          assert(FrequencyIntensityStreamWithList.isFirstLine(testTup) == false)
        }
      }
    }

    describe(".convertFileContentsToMeanIntensities") {
      it("iterates over the file and returns a list of lists") {
        //val lOfLists = FrequencyIntensityStreamWithList.convertFileContentsToMeanIntensities(freqFile).toList
        //assert(lOfLists.size == freqFileGroupCount)
        //lOfLists.foreach { l =>
          //assert(l.size == freqIntenseCount)
        //}
      }
    }
  }
}
