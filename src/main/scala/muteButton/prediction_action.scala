package muteButton

import sys.process._
object PredictionAction {
  val negativeCase = () => toggleSystem("neg")
  val positiveCase = () => toggleSystem("ops")

  private def toggleSystem(soundControl : String) = {
    "echo '"+soundControl+"'>> predictions" !
  }
}
