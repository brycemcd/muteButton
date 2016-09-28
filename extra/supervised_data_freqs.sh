#!/bin/bash

# This script converts audio files into frequency/intensity text files

basedir=/media/brycemcd/filestore/spark2bkp/football/supervised_samples

catArr=(game ad)
for c in ${catArr[@]}; do
  echo "entering category $c"
  gameFiles=$(ls $basedir/$c/*.wav)

  for f in $gameFiles; do
    echo "analyzing $f"
    filename=$(basename "$f")
    extension="${filename##*.}"
    filename="${filename%.*}"

    sox $f -n stat -freq 2>$basedir/$c/freqs/${filename}_freqs.txt
  done
done
