As of 2016-09-10, This is the directory structure for the data. The
structure is very unstable at the moment as I keep finding new ways
to organize the data according to the evolving needs.

Broadly:

+ most raw audio capture data is in ./
+ frequency-intensity data is mostly in freq directories
+ trained data is in /ad, /game, /unsure, /both directories
+ bkp is shorthand for backup. I am very risk averse to losing trained
  data


```text
.
./freqs
./freqs/exp
./supervised_samples
./supervised_samples/unsure
./supervised_samples/ad
./supervised_samples/ad/freqs
./supervised_samples/both
./supervised_samples/game
./supervised_samples/game/freqs
./supervised_samples/game/freqs/bkp
```
