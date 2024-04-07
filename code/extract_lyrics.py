import spotipy
import argparse
import os
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy import SpotifyException
import lyricsgenius
import pandas as pd
import time
import re

from requests.exceptions import ReadTimeout, Timeout

class SpotifyExtractor:
    """Class to extract metrics and lyrics from Spotify songs"""
    def __init__(self, genius_acess, spotipy_client_id, spotipy_client_secret):
        """
        Initializes the class.
        Attributes:
            genius_acess: key to use genius API
            spotipy_client_id: Spotify app id to use
            spotipy_client_secret: Spotify app secret key
        """

        self.genius = lyricsgenius.Genius(genius_acess)
        self.sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=spotipy_client_id, client_secret=spotipy_client_secret))
    
    def _get_playlist_tracks(self, playlist):
        """Get tracks from playlist
        Attributes:
            playlist: spotipy playlist object
        Returns:
            tracks: list containing playlist tracks
        """
        tracks = playlist['items']
        while playlist['next']:
            playlist = self.sp.next(playlist)
            tracks.extend(playlist['items'])
        return tracks

    def extrae_full(self, playlists):
        """
        Extract all info (audio features and lyrics) from tracks in the playlists.
        Attributes:
            playlists: list containing playlists' URLs to extract info from
        Returns:
            data: pandas Dataframe containing track info.
        """
        if isinstance(playlists, str):
            playlists = [playlists]
        
        try:
            playlists_tracks = [self.sp.playlist_items(url,limit = 100) for url in playlists]
        except Exception as e:
            print(e)

        playlists_tracks_full = [self._get_playlist_tracks(playlist) for playlist in playlists_tracks]
        ids, canciones = [], []

        con_letra, sin_letra = 0, 0
        for playlist in playlists_tracks_full:
            for track in playlist:
                if track['track']:
                    # Initialise empty dict
                    cancion = {key:None for key in ["track_name", "artist_name", "track_id", "lyrics"]}
                    track_info = track['track']

                    # Check if song has been added
                    if track_info["id"] in ids:
                        print("La canción {} del artista {} ya ha sido añadida.".format(track_info["name"], track_info['artists'][0]['name']))
                    if track_info["id"] not in ids:

                        cancion["artist_name"] = track_info['artists'][0]['name']
                        cancion["track_name"] = track_info['name']
                        cancion["track_id"] = track_info["id"]
                        
                        if cancion["artist_name"] is None or cancion["track_name"] is None:
                            print(str(track["track"]))

                        # Extract track's audio features
                        keep_keys = ['danceability','energy','key','loudness''mode','speechiness','acousticness','instrumentalness','liveness','valence','tempo','duration_ms','time_signature']
                        try:
                            time.sleep(5)
                            info_sonido = self.sp.audio_features(track_info["id"])

                            for key in info_sonido[0].keys():
                                if key in keep_keys:
                                    cancion[key] = info_sonido[0][key]
                        except SpotifyException as e:
                            print(e)
                            for key in keep_keys:
                                cancion[key] = None
                        
                        # Extract track lyrics using Genius
                        try:
                            time.sleep(0.05)
                            song = self.genius.search_song(track_info["name"], track_info['artists'][0]['name'])
                        except Timeout as e:
                            error_message = str(e)
                            # Match timeout in seconds from error message
                            match = re.search(r'read timeout=(\d+)', error_message)
                            if match:
                                timeout_seconds = int(match.group(1))
                                print(f"Aplicando timeout de {timeout_seconds} en la llamada a Genius")
                                time.sleep(timeout_seconds)
                                song = self.genius.search_song(track_info["name"], track_info['artists'][0]['name'])

                        if song is not None:
                            con_letra += 1
                            print("Encontradas {} canciones con letra".format(con_letra))
                            # print(f"Canción: {track_info['track_name']} - Artista: {track_info['artist_name']}, letra: {song.lyrics[:50]}")

                            cancion["lyrics"] = song.lyrics
                        else:
                            sin_letra += 1
                            print("Encontradas {} canciones sin letra".format(sin_letra))
                    
                    # Add track id to id list so duplicates are not added
                    ids.append(track_info["id"])

                    canciones.append(cancion)

        print(f"Encontradas {con_letra} canciones con letra y {sin_letra} canciones sin letra")
        
        # Drop songs with no data, ie, no song lyrics nor sound info.
        data = pd.DataFrame(canciones).dropna(how="all")
        
        return data

    def solo_nombres(self, playlists):
        """
        Extract only name and artist from tracks in the playlists.
        Attributes:
            playlists: list containing playlists' URLs to extract info from
        Returns:
            output: list containing tuples formed by artist name and track name for all tracks in playlists
        """
        playlists_tracks = [self.sp.playlist_tracks(url,limit = 100) for url in playlists]

        playlists_tracks_full = [self._get_playlist_tracks(playlist) for playlist in playlists_tracks]

        artistas = [track['track']['artists'][0]['name'] if track['track'] else None for playlist in playlists_tracks_full for track in playlist]
        nombres = [track['track']['name'] if track['track'] else None for playlist in playlists_tracks_full for track in playlist]

        output = list(zip(artistas, nombres))
        return output

if __name__ == "__main__":
    load_dotenv()
    extractor = SpotifyExtractor(genius_acess=os.environ["GENIUS_ACESS"],
                                 spotipy_client_id=os.environ["SPOTIPY_CLIENT_ID"],
                                 spotipy_client_secret=os.environ["SPOTIPY_CLIENT_SECRET"])
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-playlists", help="list containing playlists directories")
    parser.add_argument("-output", help="output file for csv file")

    args = parser.parse_args()

    out_df = extractor.extrae_full(args.playlists)

    out_df.to_csv(args.output)