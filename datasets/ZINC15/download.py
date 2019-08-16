import argparse
import logging
import os
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

_zinc15_dir = Path("/publicdata/tpp/datasets/ZINC15")
_sdf_path = _zinc15_dir / "sdf"

_zinc15_url = (
    "https://zinc15.docking.org/activities.sdf"
    ":zinc_id+gene_name+organism+num_observations+affinity+smiles"
)
_zinc15_n_items = 638174
_zinc15_items_per_page = 100


def get_page_as_sdf(page: int, sdf_path: Path, url: str = _zinc15_url):
    r = requests.get(url, params={"page": page})
    if r.status_code == requests.codes.ok:
        page_path = sdf_path / f"page{page}"
        with open(page_path.as_posix(), "wb") as sdf:
            sdf.write(r.content)


def download_zinc15(sdf_path: Path):
    logging.basicConfig(level=logging.DEBUG)

    # compute number of pages
    n_pages = _zinc15_n_items // _zinc15_items_per_page + 1

    # create new folder for SDF files
    if not sdf_path.exists():
        sdf_path.mkdir()

    pages = range(1, 1 + n_pages)
    failed_pages = []

    # if a file named 'failures.csv' exits, parse the failed pages
    # and retry to download them
    if os.path.exists("failures.csv"):
        pages = pd.read_csv("failures.csv", header=None).iloc[:, 0].to_list()

    for page in tqdm(pages):
        try:
            get_page_as_sdf(page, sdf_path)
        except Exception as e:
            logging.error(e, exc_info=True)
            failed_pages.append(page)

    logging.info(f"Downloaded pages with {len(failed_pages)} failures")

    with open("failures.csv", "w") as outfile:
        for failure in failed_pages:
            outfile.write(f"{failure}\n")

    logging.info("Written failed pages to `failures.csv`")
    logging.info("Rerun the script will attempt to download pages in `failures.csv` again")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ZINC15 SDF Downloader")
    parser.add_argument(
        "--output",
        type=str,
        dest="sdf_path",
        default=_sdf_path,
        help="Path where downloaded `sdf` should be written written to",
    )

    args = parser.parse_args()

    download_zinc15(sdf_path=args.sdf_path)
