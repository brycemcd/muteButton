# Mute Button

A machine learning project to listen to audio input of devices (like a television)
and mute the commercials automatically.

This project is meant to develop an end-to-end learning system capable of training
on new data sources, developing sophisticated models, "listening" to new
sources and making predictions.

## How the System Works

Source audio => translation into frequency intensities => pipe into socket =>
read data from socket for model creation/prediction => output prediction =>
toggle audio output based on prediction


## Read Source Audio

For training, audio can be piped in from previous recordings. It can be connected
via a line in port or played directly from sox. I currently use this command to
play files:

`sox /path/to/recording -p`

The output should be piped to another sox command for parsing the audio
frequencies and creating frequency/intensity data.

In production when the system should be predicting values based on the input,
audio data can be read from a sound card device using the following sox command:

`sox -t alsa hw:0 -p`

Note that -p pipes the audio to the next command. The input device can be chosen
based on settings discovered from the alsa mixer using this command:

`alsamixer`

There, various input and output devices can be seen.

## Translation Into Frequency Intensities

Sox is a powerful program and is used to receive the incoming audio stream and
convert it to a stream of frequency/intensity data. This stream should take the form:

```
0.000 0.123
1.234 3.123
```

Where the left column is the frequency and the right column is the intensity.
This text data can be piped into a network socket where the learning algorithm
can read it and make predictions.

During training, the frequency/intensity files can be saved and accessed in a
series of training scripts

## Accessing Stream Data From Socket

During prediction, frequency/intensity data can be read from a socket using
Spark Streaming's capabilities, parsed and applied to a previously generated
model to make a prediction.

The same frequency's intensity information is received multiple times each second. To
create a simplified process, The data is collected over the course of the time window
and each frequency's intensity is averaged to produce a single vector that is then
used for the prediction. Empirical experience may prove that an update to this
processing be made.

Playing a recording and piping the frequency/intensity data into a socket can be
achieved with the following command:

`sox /path/to/recording -p | sox - -n remix 1,2 remix 1,2 stat -freq 2>&1 | nc 10.1.2.230 9999`

## Making a Prediction

Data streamed over the time window used in Spark Streaming is summarized into a
single vector and then predicted against a model that was previously trained.

An output of 1 or 0 is produced. If 0, then the audio stream out of the device
should be muted, 1 should be unmuted.

## Muting / Unmuting The Output Audio Source

The output source can be controlled from system commands. Again, relying on
alsa, the command to mute/unmute is:

`amixer -q -c 1 -D pulse sset Master mute`

Where -c 1 is the audio card selected for the output stream.


---

## Training

Training data is challenging. To train a model, data is needed for both positive
and negative examples. Training is monotonous. I break up long files into 10
second increments using the following sox command provided by an answer in
[this forum.](http://sox.10957.n7.nabble.com/Split-a-big-file-I-m-recording-in-smaller-pieces-td4774.html)

> Will generate output001.ogg, output002.ogg, ouput003.ogg ,etc....... to ouput_finish.ogg

`sox dal_gb_20151213 dal_gb_20151213_chunked.wav trim 0 10 : newfile : restart`


This will generate several 10 second snippets of the larger file. `/extra/supervise_audio_file.sh`
Can be used to playback the 10 second snippets at a playback speed of 3x. Key
commands then move the files into positive and negative example directories
on the source system.

After a number of training examples have been produced, the audio sample should
be extracted into frequency/intensity data files. This can be done by running `./extra/supervised_data_freqs.sh`

These data files can be read in to the learner using a slightly
modified code path. It's the same data, but not streamed over a socket. Each
set of frequencies can be vectorized and labeled.

Each file has multiple audio samples included.  Each sample needs to be
labeled in order to ensure that when the data is read
in and distributed across the cluster that it is combined correctly when it's
vectorized as a training sample. The easiest way I've found to do this as of now
is to separate each sample into its own file and then add a third column to each file
with its filename. When the training routine runs, it will group each frequency
intensity tuple together by its filename and output the correct vector.

To label each freq/intensity file so it's suitable for training, run the
`extra/split_training_freqs.sh` command.

At this point, all the `*-labeled` files can be recombined into a single labeled
data file. This is helpful to reduce disk I/O during the training phase.

## Testing The System

1. Start TCP server `ruby extra/tcp_server.rb`

2. Start Prediction Software `sbt run`

3. Feed audio data into prediction software

(in this case, the sound file is being read in from another system)

`ssh brycemcd@10.1.2.102 "sox /home/brycemcd/Desktop/dal_gb_20151213 -p" | sox - -n remix 1,2 stat -freq 2>&1 | nc 10.1.2.230 9999`

## Errata And Old Notes:

### Generate Data For Modeling

`sox dal_gb_20151213 -n stat -freq > freqs.txt 2<&1`

### Send Audio Data to Spark Streaming Socket

` sox dal_gb_20151213 -n remix 1,2 stat -freq 2<&1 | nc -lk 9999`

### Split one audio file into lots of small files (for supervising)

[helpful source](http://sox.10957.n7.nabble.com/Split-a-big-file-I-m-recording-in-smaller-pieces-td4774.html)

`sox infile.ogg output.ogg trim 0 10 : newfile : restart`

"Will generate output001.ogg, output002.ogg, ouput003.ogg ,etc....... to ouput_finish.ogg."

### Combining audio files

`sox -m *.wav all_files.wav`

```bash
# from my desktop to spark4 server
 ssh spark4.thedevranch.net "sox /media/brycemcd/filestore/spark2bkp/football/ari_phi_chunked069.wav -p" | play - -n stat -freq >/dev/null 2>&1 | nc localhost 9999
ssh spark4.thedevranch.net "sox /media/brycemcd/filestore/spark2bkp/football/game_and_ad.wav -p" | play - -n stat -freq >/dev/null 2>&1 | nc -k 10.1.2.230 9999
```

```bash
split ad_and_game.txt -l 2048 freq
for f in freqa*; do sed -i "s:$:  $f:" $f; done
```

```bash
# plays audio from TV into computer, saves file for later processing,
# outputs frequencies and pipes audo into stereo
sudo sox -t alsa hw:3 -p gain -10 | tee car_den_3 | sudo sox - -t alsa hw:2 stat -freq gain -10 2>&1 | nc 10.1.2.230 9999
```

