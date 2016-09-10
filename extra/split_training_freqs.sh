#!/bin/bash

# This script converts freq/intens text files into labeled samples with the format
# freq intensity label

basedir=/media/brycemcd/filestore/spark2bkp/football/supervised_samples
catArr=(ad game)

returnDir=$(pwd)

for c in ${catArr[@]}; do
  cd $basedir/$c/freqs
  gameFiles=$(ls *freqs.txt)

  echo "labeling text files in $c"

  for f in $gameFiles; do
    filename=$(basename "$f")
    extension="${filename##*.}"
    filename="${filename%.*}"

    # split file
    split ${filename}.${extension} -l 2048 $filename

    # add filename as a column
    # NOTE: This directory will contain raw txt files that should NOT be labeled
    # by this script
    for labeledFile in $(ls *freqs[a-z]*)
    do
      sed -i "s:$:  $labeledFile:" $labeledFile
    done

    # recombine
    cat *freqs[a-z]* > $filename-labeled.txt

    # remove artifacts
    rm *freqs[a-z]*

    echo "labeled $filename"

  done
done
cd $returnDir
