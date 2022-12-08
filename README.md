# lastfm-challenge

## Run the application

1. Build the docker image. We need to pass the host user's `uid` to the build to avoid file permission conflicts

``` bash

docker build -t lastfm_pyspark --build-arg USER_ID=$(id -u) .

```

2. Create a directory on the host for the output file

``` bash

mkdir ./out

```

3. Run the app!

``` bash

docker run --rm -v $(pwd)/out:/out lastfm_pyspark

```

## Asumptions

  - The application runs in a container engine (e.g. Docker, Kubernetes)

  - Session time is calculated based on play actions (i.e. regardless or the last track's duration). More accurate session times could be calculated if the play duration was provided in the dataset. In this case session duration should be calculated from the start of the first track to the end of the last track.



## Improvements

  - If the application is intended to evolve, it would be a good practice to create a base docker image with the dataset and the dependencies. This could significantly 
  speed up the CI/CD process.

  - Similarly, storing the data in parquet format within the image would drastically reduce execution times.

  - Because of file permissions conflicts (different `uid`'s between the host and the container), the current solution would require a different build per host user. If the application is intended to be run in different environments, a more elegant solution would consist on a custom entrypoint script that accepts a `uid` argument at container creation time. This way, the `spark` user will be created dynamically at runtime with the `uid` of the calling user.
  
## Development

Please install the dependencies listed in `requirements-dev.txt`.

``` bash

pip install -r requirements-dev.txt

```

Run the tests and static type checks before pushing changes

``` bash

python -m pytest ./test  # Unit tests
mypy lastfm_pyspark      # Type checks

```
