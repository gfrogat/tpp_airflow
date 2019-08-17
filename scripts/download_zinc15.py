#!/bin/env python
import argparse
import logging
import os
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

_zinc15_dir = Path("/publicdata/tpp/datasets/ZINC15")
_sdf_path = _zinc15_dir / "sdf"

ZINC15_URL = (
    "https://zinc15.docking.org/activities.sdf"
    ":zinc_id+gene_name+organism+num_observations+affinity+smiles"
)
ZINC15_NUM_ITEMS = 638174
ZINC15_ITEMS_PER_PAGE = 100


def get_page_as_sdf(page: int, sdf_path: Path, url: str = ZINC15_URL):
    r = requests.get(url, params={"page": page})
    if r.status_code == requests.codes.ok:
        page_path = sdf_path / f"page{page}"
        with open(page_path.as_posix(), "wb") as sdf:
            sdf.write(r.content)


def download_zinc15(sdf_path: Path):
    logging.basicConfig(level=logging.DEBUG)

    # compute number of pages
    n_pages = ZINC15_NUM_ITEMS // ZINC15_ITEMS_PER_PAGE + 1

    # create new folder for SDF files
    if not sdf_path.exists():
        sdf_path.mkdir()

    pages = range(1, 1 + n_pages)
    failed_pages = []

    # if a file named 'failures.csv' exits, parse the failed pages
    # and retry to download them
    failures_path = Path("failures.csv")
    if failures_path.exists():
        pages = pd.read_csv("failures.csv", header=None).iloc[:, 0].to_list()

    for page in tqdm(pages):
        try:
            get_page_as_sdf(page, sdf_path)
        except Exception as e:
            logging.error(e, exc_info=True)
            failed_pages.append(page)

    logging.info(f"Downloaded pages with {len(failed_pages)} failures")

    with open(failures_path, "w") as outfile:
        for failure in failed_pages:
            outfile.write(f"{failure}\n")

    logging.info(f"Written failed pages to {failures_path}")
    logging.info(("Rerunning this script will attempt to download"
                  f"failed pages in {failures_path} again"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="ZINC15 SDF Downloader",
        description="Download ZINC15 BioAssays in SDF format.")
    parser.add_argument(
        "--output",
        type=str,
        dest="sdf_path",
        default=_sdf_path,
        help="Path to store downloaded SDF files"
    )

    args = parser.parse_args()

    download_zinc15(sdf_path=args.sdf_path)
