import argparse
from yt_dlp import YoutubeDL
import pandas as pd

def download_songs(query, download_directory='.'):
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
                video = ydl.extract_info(f"ytsearch:{query}", download=False)['entries'][0]['id']
                info = ydl.extract_info(video)
                filename = ydl.prepare_filename(info)
                if not filename:
                    print(f"Track Not Found⚠️")
                else:
                    path_link = filename
                    return path_link
        except Exception as e:
            print(f"Error procesando {query}", "\n", e)
            video = None
            
        return video

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-csv_song_path", help="path to csv songs")
    parser.add_argument("-download_directory", help="directory to download the song into")

    args = parser.parse_args()

    df = pd.read_csv(args.csv_song_path)

    song_names = df["track_name"] + df["artist_name"]

    for song_name in song_names:
        download_songs(song_name, args.download_directory)