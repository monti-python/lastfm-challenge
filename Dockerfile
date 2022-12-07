FROM python:3.10-slim-bullseye
ARG DATASET_URL=http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz
# Set the application directory
RUN mkdir /app
WORKDIR /app
# Set the output directory
RUN mkdir /out
# Install dependencies
COPY ./requirements.txt ./requirements-dev.txt /app/
RUN pip install -r requirements.txt -r requirements-dev.txt
# Install jdk and download the dataset
RUN apt-get update \
    && apt-get install -y openjdk-11-jdk curl \
    && curl -o lastfm-dataset-1K.tar.gz $DATASET_URL \
    && tar -xvzf lastfm-dataset-1K.tar.gz \
    && rm lastfm-dataset-1K.tar.gz \
    && apt-get remove -y curl && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
# Copy the source and test code
COPY ./lastfm_pyspark /app/lastfm_pyspark
COPY ./test /app/test
COPY ./pytest.ini /app/
# Run the tests and mypy
RUN pytest test && mypy lastfm_pyspark
# Set entrypoint
ENTRYPOINT ["python", "lastfm_pyspark/main.py"]

