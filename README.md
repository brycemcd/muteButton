# Mute Button



## Generate Data
`sox dal_gb_20151213 -n remix 1,2 stat -freq > freqs.txt 2<&1`

## Send Audio Data to Spark Streaming Socket
` sox dal_gb_20151213 -n remix 1,2 stat -freq 2<&1 | nc -lk 9999`

## Supervising Files
```bash
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
```

## Combining audio files

`sox -m *.wav all_files.wav`

```bash
# from my desktop to spark4 server
 ssh spark4.thedevranch.net "sox /media/brycemcd/filestore/spark2bkp/football/ari_phi_chunked069.wav -p" | play - -n stat -freq >/dev/null 2>&1 | nc localhost 9999
ssh spark4.thedevranch.net "sox /media/brycemcd/filestore/spark2bkp/football/game_and_ad.wav -p" | play - -n stat -freq >/dev/null 2>&1 | nc -k 10.1.2.230 9999
```
