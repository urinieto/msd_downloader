#!/usr/bin/env python
"""
Downloads the MSD preview tracks in parallel.
"""
import argparse
from joblib import Parallel, delayed
import logging
import os
import subprocess
import time


def download_subfolder(url, index):
    char = str(unichr(index + ord('A')))
    sub_url = os.path.join(url, char) + "/"
    subprocess.call(["wget", "-r", "-nH", sub_url])


def process(url, n_jobs):
    N = 26
    return Parallel(n_jobs=n_jobs)(delayed(download_subfolder)(
        url, i) for i in range(N))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Downloads the MSD preview tracks in parallel.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("url",
                        action="store",
                        help="URL where the MSD preview tracks are.")
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
    process(args.url, args.n_jobs)

    # Done!
    logging.info("Done! Took %.2f seconds." % (time.time() - start_time))
