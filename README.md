# lastfm-challenge

## Run the application

First of all, build the docker image

``` bash

docker build -t lastfm_pyspark .

```

Afterwards, create a directory for output

``` bash

mkdir ./out

```

Finally, run the app!

``` bash

docker run --rm -v $(pwd)/out:/out -u $(whoami) lastfm_pyspark

```

## Asumptions

  - Session time is calculated based on play actions (i.e. regardless or the last track's duration). More accurate session times could be calculated if the timestamp when tracks end was provided in the dataset. In this case session duration should be calculated from the start of the first track to the end of the last track.

  

## Improvements