FROM python:3.10-slim-bullseye

ARG DATASET_URL=http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz
ARG USER_ID
# Set the application directory
RUN mkdir /app
WORKDIR /app
# Set the output directory
RUN mkdir /out
# Install dependencies and download the dataset
RUN apt-get update \
    && apt-get install -y openjdk-11-jdk procps curl \
    && curl -o lastfm-dataset-1K.tar.gz $DATASET_URL \
    && tar -xvzf lastfm-dataset-1K.tar.gz \
    && rm lastfm-dataset-1K.tar.gz \
    && apt-get remove -y curl && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
# Set the user
RUN useradd -m -u ${USER_ID} spark && chown -R spark /app /out
USER spark
# Install dependencies
COPY ./requirements.txt /app/
RUN pip install --user -r requirements.txt
# Copy the source code
COPY ./lastfm_pyspark /app/lastfm_pyspark
# Set entrypoint
ENTRYPOINT ["python", "lastfm_pyspark/main.py"]

