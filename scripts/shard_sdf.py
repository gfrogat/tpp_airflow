#!/bin/env python
import argparse
import logging
from functools import partial
from multiprocessing import Pool, TimeoutError, cpu_count
from pathlib import Path
from typing import Tuple

from rdkit import Chem
from tqdm import tqdm

from tpp.utils.argcheck import check_input_path, check_parent_folder

logging.basicConfig(level=logging.INFO)

NUM_SHARDS = 100
NUM_PROC = 6


def export_sdf_shard(
    shard_meta: Tuple[int, Tuple[int, int]],
    sdf_path: Path,
    shards_path: Path,
    width: int = 2,
):
    shard_id, item_range = shard_meta

    filename = shards_path / f"{sdf_path.stem}_shard{shard_id:0{width}d}.sdf"

    logging.info(f"Exporting shard `{filename}`")
    sdf_reader = Chem.SDMolSupplier(sdf_path.as_posix())
    sdf_writer = Chem.SDWriter(filename.as_posix())

    for idx in range(*item_range):
        mol = sdf_reader[idx]
        if mol is not None:
            sdf_writer.write(mol)

    sdf_writer.close()


def shard_sdf(sdf_path: Path, shards_path: Path, num_shards: int, num_proc: int):
    suppl = Chem.SDMolSupplier(sdf_path.as_posix())

    logging.info(f"Getting number of items in `{sdf_path}`. This can take a while.")
    num_items = len(suppl)
    shard_size = num_items // num_shards + 1
    logging.info(f"{shard_size} items per shard")
    logging.info(f"Total: {num_items} items")

    if not shards_path.exists():
        shards_path.mkdir()

    # Setup metadata for constructing shards
    shard_meta = [
        (shard_id, (item_id, min(item_id + shard_size, num_items)))
        for (shard_id, item_id) in zip(
            range(num_shards), range(0, num_items, shard_size)
        )
    ]

    with tqdm(total=num_shards) as progress_bar:
        with Pool(num_proc) as pool:
            it = pool.imap_unordered(
                partial(
                    export_sdf_shard,
                    sdf_path=sdf_path,
                    shard_dir=shards_path,
                    width=len(str(num_shards)),
                ),
                shard_meta,
                chunksize=1,
            )
            try:
                while True:
                    try:
                        it.next(timeout=300)
                    except TimeoutError as error:
                        logging.error(error, exc_info=True)

                    progress_bar.update(1)
            except StopIteration:
                logging.info(f"Finished sharding of `{sdf_path}`")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="ChEMBL SDF Sharder",
        description="Shard ChEMBL SDF file into multiple smaller files.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        dest="sdf_path",
        help=f"Path to ChEMBL `SDF` file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        dest="shards_path",
        help=f"Path where `SDF` shards should be written to",
    )
    parser.add_argument(
        "--num-shards",
        type=int,
        dest="num_shards",
        default=NUM_SHARDS,
        help=f"Number of shards to create (default: {NUM_SHARDS}",
    )
    parser.add_argument(
        "--num-proc",
        type=int,
        dest="num_proc",
        default=NUM_PROC,
        help=f"Number of processes for exporting (default: {NUM_PROC}",
    )

    args = parser.parse_args()

    check_input_path(args.sdf_path)
    check_parent_folder(args.shards_path)

    if args.num_proc > cpu_count():
        raise OSError("Number of processes exceeds number of available CPU cores!")

    shard_sdf(args.sdf_path, args.shards_path, args.num_shards, args.num_proc)
