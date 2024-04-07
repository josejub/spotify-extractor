import s3fs
import argparse
from dotenv import load_dotenv
import os

if __name__ == "__main__":
    load_dotenv()

    s3 = s3fs.core.S3FileSystem(endpoint_url=os.getenv("AWS_ENDPOINT"),
                            key=os.getenv("AWS_ACCESS_KEY_ID"),
                            secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
                            )

    parser = argparse.ArgumentParser()
    parser.add_argument("-local_dir", help="directory to upload")
    parser.add_argument("-s3_dir", help="s3 directory to upload to")

    args = parser.parse_args()

    s3.put(args.local_dir, args.s3_dir, recursive=True) 