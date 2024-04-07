# spotify-extractor
ETL project using airflow for spotify and youtube_downloader orquestation and loading into minio (local s3 storage)

# Requirements
- A working installation of Docker 

# Steps to use the project
1. Build the extractor docker image.
docker build -t spotify_extractor .
docker compose up -d 