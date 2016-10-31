#!/usr/bin/env python
"""
Downloads the MSD preview tracks in parallel.
"""
import argparse
from joblib import Parallel, delayed
import logging
import pandas as pd
import os
import subprocess
import time
import urllib


def ensure_dir(directory):
    """Makes sure that the given directory exists."""
    if not os.path.exists(directory):
        os.makedirs(directory)


def download_subfolder(url, index):
    char = str(unichr(index + ord('A')))
    sub_url = os.path.join(url, char) + "/"
    subprocess.call(["wget", "-r", "-nH", sub_url])


def download_file(audio_path, url, out_dir):
    # Get file paths
    remote_file = os.path.join(url, audio_path)
    out_file = os.path.join(out_dir, audio_path)

    # Do nothing if path is a directory
    if os.path.isdir(out_file):
        return

    # Make sure the directory exists
    ensure_dir(os.path.dirname(out_file))

    # Download actual file if doesn't exist yet
    if not os.path.isfile(out_file):
        logging.info("Downloading {} into {} ...".format(remote_file, out_file))
        urllib.urlretrieve(remote_file, out_file)


def process(url, data_file, n_jobs, out_dir):
    """Main process to download the files."""
    if data_file is None:
        N = 26
        return Parallel(n_jobs=n_jobs)(delayed(download_subfolder)(
            url, i) for i in range(N))
    else:
        df = pd.read_csv(data_file, sep="\t", header=None)
        df.columns = ["size", "audio_path"]
        for index, row in df.iterrows():
            download_file(row["audio_path"], url, out_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Downloads the MSD preview tracks in parallel.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("url",
                        action="store",
                        help="URL where the MSD preview tracks are.")
    parser.add_argument("-d",
                        action="store",
                        dest="data_file",
                        default=None,
                        type=str,
                        help="Path to the file containing the list of all audio files.")
    parser.add_argument("-o",
                        action="store",
                        dest="out_dir",
                        default="./msd/mp3/",
                        type=str,
                        help="Path to the output directory.")
    parser.add_argument("-j",
                        action="store",
                        dest="n_jobs",
                        default=26,
                        type=int,
                        help="The number of threads to use (max 26)")

    args = parser.parse_args()
    start_time = time.time()

    # Setup the logger
    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s',
                        level=logging.INFO)

    # Call main process
    process(args.url, args.data_file, args.n_jobs, args.out_dir)

    # Done!
    logging.info("Done! Took %.2f seconds." % (time.time() - start_time))
