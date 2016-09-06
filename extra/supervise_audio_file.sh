#!/bin/bash

for i in $(seq -s "  " -w 21 30); do
branch:  - last commit: 
  export base="/media/brycemcd/filestore/spark2bkp/football"
  export fpath="$base/ari_phi_chunked0${i}.wav"

  ssh spark4.thedevranch.net sox $fpath -t sox - tempo 3 | sox -q -t sox
- -d

  espeak -v en "that was $i"
  echo -n "Enter some text > "
  read text
  echo "You entered: $text"
  if [ $text = "g" ]; then
    echo "game!"
    ssh spark4.thedevranch.net mv $fpath "$base/game"
  elif [ $text = "a" ]; then
    echo "ad!"
    ssh spark4.thedevranch.net mv $fpath "$base/ad"
  elif [ $text = "b" ]; then
    echo "both"
    ssh spark4.thedevranch.net mv $fpath "$base/both"
  else
    echo "something else"
  fi
done
