from datetime import datetime
import logging
import tempfile
import os
import time

import sys

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.decorators import task
from airflow.models.taskinstance import TaskInstance
from docker.types import Mount

default_args = {
    'owner': 'josejub',
    'start_date': datetime.now()
}
logger = logging.Logger("extract_spotify")
playlists = [
            "https://open.spotify.com/playlist/0c3djV2XAgsWOm5KOHIZ6A?si=6f034a2e03494372",
            "https://open.spotify.com/playlist/37i9dQZF1DWXYWuNDAdJsw?si=83de61e9d5884a17",
            "https://open.spotify.com/playlist/7ttWqgppJ9J4rGjSsJRHu1"
            # "https://open.spotify.com/playlist/4qiSIwTZAqZB07czaJxi0p?si=23d91893e4c44b8a",

            ]

with DAG(
    dag_id='extract_spotify',
    default_args = default_args,
    schedule_interval=None
) as dag:
    run_id = "{{ ts_nodash }}"

    # Create default mount to be used in all containers. It is easier than bind mounts, as task 
    # containers are launched in the host machine instead of the airflow container.
    temp = f"/temp"
    mounts = [
        Mount(source="temp", target="/temp"), 
            #   Mount(source="code", target="/code")
              ]
    
    # Define playlist independent tasks
    setup = BashOperator(task_id = "setup", bash_command=f"mkdir -p {temp}/{run_id}/csv; mkdir -p {temp}/{run_id}/mp3")
    join = DockerOperator(task_id=f"join_files",
                            image = f"spotify_extractor", 
                            command=f"python /code/join.py -csv_folder {temp}/{run_id}/csv -out_path {temp}/{run_id}/{run_id}.csv", 
                            mounts=mounts,
                            auto_remove=True
                            )
    for i, playlist in enumerate(playlists):
        mid_csv_path = f"{temp}/{run_id}/csv/out{i}.csv"
        # For each playlist define the docker task to extract audio info and audio files
        lyrics_extractor = DockerOperator(task_id=f"lyrics_extractor_{i}",
                                    image = f"spotify_extractor", 
                                    command=f"python /code/extract_lyrics.py -playlists {playlist} -chunk_size 1 -download_dir {temp}/{run_id}/mp3 -run_id {run_id} -output {mid_csv_path}", 
                                    mounts=mounts,
                                    auto_remove=True
                                    )
        # audio_extractor = DockerOperator(task_id=f"audio_extractor_{i}",
        #                             image = f"spotify_extractor", 
        #                             command=f"python /code/extract_audio.py -csv_song_path {mid_csv_path} -download_directory ", 
        #                             mounts=mounts,
        #                             auto_remove=True
        #                             )
        # STEP 1: Create temp directories to hold files until they are loaded into s3
        # STEP 2: Extract lyrics and audio metrics for the playlist        
        setup.set_downstream(lyrics_extractor)
        # STEP 3: Extract audio files for the playlist
        # lyrics_extractor.set_downstream(audio_extractor)
        # STEP 4: join lyrics and audio metrics extracted in step 2 to produce a single csv per run
        lyrics_extractor.set_downstream(join)
    
    # Define task to load temp run folder to s3
    load_to_s3 = DockerOperator(task_id=f"load_to_s3",
                                image = f"spotify_extractor", 
                                command=f"python /code/upload_to_s3.py -local_dir {temp}/{run_id} -s3_dir spotify-extractor/{run_id}", 
                                mounts=mounts,
                                network_mode="extractor_net",
                                auto_remove=True
                                )
    # STEP 5: Load temp run folder to s3.
    join.set_downstream(load_to_s3)

    # STEP 6: remove run folder from local storage, as it is already in s3.
    cleanup = BashOperator(task_id = "cleanup", bash_command=f"rm -rf {temp}/{run_id}")
    load_to_s3.set_downstream(cleanup)