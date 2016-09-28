#!/bin/bash

#for i in $(seq -s "  " -w 21 30); do

fs=$(ssh spark4.thedevranch.net "ls /media/brycemcd/filestore/spark2bkp/football/unsupervised/*chunked*wav")

for f in $fs; do
  base="/media/brycemcd/filestore/spark2bkp/football"
  supervised_base="$base/supervised_samples"
  export fpath="$base/ari_phi_chunked0${i}.wav"

  ssh spark4.thedevranch.net sox $f -t sox - tempo 2 | sox -q -t sox - -d

  echo -n "Type a if that was an ad, g if that was a game, b if both and u if unsure > "
  read text

  echo "You entered: $text"
  if [ $text = "g" ]; then
    echo "game!"
    ssh spark4.thedevranch.net mv $f "$supervised_base/game"
  elif [ $text = "a" ]; then
    echo "ad!"
    ssh spark4.thedevranch.net mv $f "$supervised_base/ad"
  elif [ $text = "b" ]; then
    echo "both"
    ssh spark4.thedevranch.net mv $f "$supervised_base/both"
  elif [ $text = "u" ]; then
    echo "unsure"
    ssh spark4.thedevranch.net mv $f "$supervised_base/unsure"
  else
    echo "something else"
  fi
done
