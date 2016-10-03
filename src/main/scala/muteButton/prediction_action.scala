package muteButton

import sys.process._
object PredictionAction {
  val negativeCase = () => toggleSystem("unmute")
  val positiveCase = () => toggleSystem("mute")

  private def toggleSystem(soundControl : String) : Int = {
    //"echo '"+soundControl+"'>> predictions" !
    "sudo amixer set Speaker " + soundControl !
  }
}
