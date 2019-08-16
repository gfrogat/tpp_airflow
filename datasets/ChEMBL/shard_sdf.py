import argparse
import logging
from functools import partial
from multiprocessing import Pool, TimeoutError, cpu_count
from pathlib import Path
from typing import Tuple

from rdkit import Chem
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)

_chembl_path = Path("/publicdata/tpp/datasets/ChEMBL/chembl_25")
_sdf_path = _chembl_path / "chembl_25.sdf"
_shards_path = _chembl_path / "chembl_25_shards"
_num_shards = 100
_num_proc = 6


def export_sdf_shard(shard_meta: Tuple[int, Tuple[int, int]], sdf_path: Path, shards_path: Path, width: int = 2):
    shard_id, item_range = shard_meta

    shard_filename = shards_path / f"{_chembl_path.stem}_shard{shard_id:0{width}d}"

    logging.info(f"Exporting shard `{shard_filename}`")
    sdf_reader = Chem.SDMolSupplier(sdf_path.as_posix())
    sdf_writer = Chem.SDWriter(shard_filename.as_posix())

    for idx in range(*item_range):
        mol = sdf_reader[idx]
        if mol is not None:
            sdf_writer.write(mol)

    sdf_writer.close()


def shard_sdf(sdf_path: Path, shards_path: Path, num_shards: int, num_proc: int):
    suppl = Chem.SDMolSupplier(sdf_path.as_posix())

    logging.info(
        f"Getting number of items in `{sdf_path}`. This can take a while.")
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
                    filename=sdf_path,
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
        pass
    logging.info(f"Finished sharding of `{sdf_path}`")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ChEMBL SDF Sharder")
    parser.add_argument(
        "--input",
        type=Path,
        dest="sdf_path",
        default=_sdf_path,
        help=f"Path to ChEMBL `SDF` file (default: {_sdf_path}",
    )
    parser.add_argument(
        "--output",
        type=Path,
        dest="shards_path",
        default=_shards_path,
        help=f"Path where `SDF` shards should be written to (default: {_shards_path}",
    )
    parser.add_argument(
        "--num-shards",
        type=int,
        dest="num_shards",
        default=_num_shards,
        help=f"Number of shards to create (default: {_num_shards}"
    )
    parser.add_argument(
        "--num-proc",
        type=int,
        dest="num_proc",
        default=_num_proc,
        help=f"Number of processes for exporting (default: {_num_proc}"
    )

    args = parser.parse_args()

    if not args.sdf_path.exists():
        raise ValueError("Input file does not exist!")

    if not args.shards_path.parent.exists():
        raise ValueError("Parent folder of shards folder does not exist")

    if args.num_proc > cpu_count():
        raise ValueError("Number of processes exceeds number of available CPU cores!")

    shard_sdf(args.sdf_path, args.shards_path, args.num_shards, args.num_proc)
