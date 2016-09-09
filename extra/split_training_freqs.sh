#!/bin/bash

# split file
split ad_and_game.txt -l 2048 freq

# add filename as a column
for f in freq*
do
  sed -i "s:$:  $f:" $f
done

# recombine
cat freq* > nyg_mia_20151214-labeled.txt

# remove artifacts
rm freq*
