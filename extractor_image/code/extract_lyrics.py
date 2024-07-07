import spotipy
import argparse
import os
import lyricsgenius
import pandas as pd
import time
import re
import s3fs

from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy import SpotifyException
from requests.exceptions import ReadTimeout, Timeout
from yt_dlp import YoutubeDL

class SpotifyExtractor:
    """Class to extract metrics and lyrics from Spotify songs"""
    def __init__(self):
        """
        Initializes the class.
        Attributes:
            genius_acess: key to use genius API
            spotipy_client_id: Spotify app id to use
            spotipy_client_secret: Spotify app secret key
        """

        self.genius = lyricsgenius.Genius(os.environ["GENIUS_ACESS"])
        self.sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=os.environ["SPOTIPY_CLIENT_ID"], client_secret=os.environ["SPOTIPY_CLIENT_SECRET"]))
        self.s3 = s3fs.core.S3FileSystem(endpoint_url=os.getenv("AWS_ENDPOINT"),
                            key=os.getenv("AWS_ACCESS_KEY_ID"),
                            secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
                            )
    
    def get_playlist_info(self, playlist_url, chunk_size, download_dir, run_id):
        """Get tracks from playlist
        Attributes:
            playlist: spotipy playlist object
        Returns:
            tracks: list containing playlist tracks
        """
        info_list = []
        playlist = self.sp.playlist_items(playlist_url,limit = chunk_size)
        for track in playlist["items"]:
                song_dict = self.extract_song(track, download_dir, run_id)
                info_list.append(song_dict)
        
        while playlist['next']:
            playlist = self.sp.next(playlist)
            for track in playlist["items"]:
                song_dict = self.extract_song(track, download_dir, run_id)
                info_list.append(song_dict)
        
        info_df = pd.DataFrame(info_list)
        return info_df

    def get_audio_features(self, track_id):
        """
        Extract track's audio features
        """

        info = {}

        keep_keys = ['danceability','energy','key','loudness''mode','speechiness','acousticness','instrumentalness','liveness','valence','tempo','duration_ms','time_signature']
        try:
            time.sleep(0.05)
            info_sonido = self.sp.audio_features(track_id)

            for key in info_sonido[0].keys():
                if key in keep_keys:
                    info[key] = info_sonido[0][key]
        except SpotifyException as e:
            print(e)
            for key in keep_keys:
                info[key] = None
        except ReadTimeout:
            print(e)
            for key in keep_keys:
                info[key] = None
        
        return info

    def get_song_lyrics(self, track_name, artist_name):
        """
        Extract track lyrics using Genius
        """
        
        try:
            time.sleep(0.05)
            song = self.genius.search_song(track_name, artist_name)
        except Timeout as e:
            error_message = str(e)
            # Match timeout in seconds from error message
            match = re.search(r'read timeout=(\d+)', error_message)
            if match:
                timeout_seconds = int(match.group(1))
                print(f"Aplicando timeout de {timeout_seconds} en la llamada a Genius")
                time.sleep(timeout_seconds)
                song = self.genius.search_song(track_name, artist_name)

        if song is not None:
            lyrics = song.lyrics
        else:
            lyrics = ""
        
        return lyrics

    def get_song_mp3(self, query, download_directory='.'):
            """
            Downloads track mp3 to disk using youtube search and yt_dlp library
            Attributes:
                - query: keyword (song title) to perform the search
                - download_directory: directory to save file in
            Returns:
                - video: mp3 file path, if the file could be downloaded
            """
            query = f"{query} Lyrics".replace(":", "").replace("\"", "")
            ydl_opts = {
                'format': "bestaudio/best",
                'default_search': 'ytsearch',
                'noplaylist': True,
                "nocheckcertificate": True,
                "outtmpl": f"{download_directory}/%(title)s.mp3",
                "quiet": True,
                "addmetadata": True,
                "prefer_ffmpeg": True,
                "geo_bypass": True,
                "nocheckcertificate": True,
            }
            try:
                with YoutubeDL(ydl_opts) as ydl:
                    # try:
                    video = ydl.extract_info(f"ytsearch:{query}", download=False)['entries'][0]['id']
                    info = ydl.extract_info(video)
                    filename = ydl.prepare_filename(info)
                    if not filename:
                        print(f"Track Not Found⚠️")
                        return ""
                    else:
                        return filename
                    
                # return video
            except Exception as e:
                print(f"Error extrayendo audio: {e}")

    def format_s3_path(self, local_path, run_id):
        file_name = re.split(r"/|\\", local_path)[-1]
        return os.getenv("S3_BUCKET") + f"{run_id}/mp3/{file_name}"
    
    def extract_song(self, track, download_dir, run_id):
        ids = []

        if track['track']:
            # Initialise empty dict
            cancion = {key:None for key in ["track_name", "artist_name", "track_id", "lyrics"]}
            track_info = track['track']

            # Check if song has been added
            if track_info["id"] in ids:
                print("La canción {} del artista {} ya ha sido añadida.".format(track_info["name"], track_info['artists'][0]['name']))
            if track_info["id"] not in ids:

                artist_name = track_info['artists'][0]['name']
                track_name = track_info['name']
                track_id = track_info["id"]
                
                if cancion["artist_name"] is None or cancion["track_name"] is None:
                    # print(str(track["track"]))
                    pass
                
                sound_info = self.get_audio_features(track_id)
                lyrics = self.get_song_lyrics(track_name, artist_name)
                local_mp3_path = self.get_song_mp3(f"{track_name} {artist_name}", download_dir)
                s3_mp3_path = self.format_s3_path(local_mp3_path, run_id)

                cancion = {"artist_name": artist_name,
                        "track_name": track_name,
                        "track_id": track_id,
                        "lyrics": lyrics,
                        "s3_mp3_path": s3_mp3_path
                        }
                cancion.update(sound_info)

        return cancion

if __name__ == "__main__":
    load_dotenv()
    extractor = SpotifyExtractor()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-playlists", help="list containing playlists directories")
    parser.add_argument("-chunk_size", help="batch length for extracted songs")
    parser.add_argument("-download_dir", help="directory to save songs in")
    parser.add_argument("-run_id", help="run id to format s3 diretories")
    parser.add_argument("-output", help="output file for csv file")

    args = parser.parse_args()

    out_df = extractor.get_playlist_info(args.playlists, args.chunk_size, args.download_dir, args.run_id)

    out_df.to_csv(args.output)
