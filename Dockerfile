FROM python:3.10-slim-bullseye
# Set the application directory
RUN mkdir /app
WORKDIR /app
# Install dependencies
COPY ./requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt
# Download the dataset
ADD http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz /app/lastfm-dataset-1K.tar.gz
RUN tar -xvzf lastfm-dataset-1K.tar.gz && rm lastfm-dataset-1K.tar.gz
