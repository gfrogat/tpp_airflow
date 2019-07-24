import logging
import os
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

_zinc15_url = (
    "https://zinc15.docking.org/activities.sdf"
    ":zinc_id+gene_name+organism+num_observations+affinity+smiles"
)
_zinc15_n_items = 638174
_zinc15_items_per_page = 100


def get_page_as_sdf(page, sdf_path, url=_zinc15_url):
    payload = {"page": page}
    r = requests.get(url, params=payload)
    if r.status_code == requests.codes.ok:
        with open(sdf_path / "page{}.sdf".format(page), "wb") as sdf:
            sdf.write(r.content)


def download_zinc15(sdf_path="/publicdata/tpp/ZINC15/ZINC15.sdf"):
    logging.basicConfig(level=logging.DEBUG)

    # compute number of pages
    n_pages = _zinc15_n_items // _zinc15_items_per_page + 1

    # create new folder for SDF files
    sdf_path = Path(sdf_path)
    if not os.path.exists(sdf_path):
        os.mkdir(sdf_path)

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

    logging.info("Downloaded pages with {} failures".format(len(failed_pages)))

    with open("failures.csv", "w") as outfile:
        for failure in failed_pages:
            outfile.write("{}\n".format(failure))
    logging.info("Written failed pages to failures.csv")


if __name__ == "__main__":
    download_zinc15()
