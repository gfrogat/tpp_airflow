from rdkit import Chem
from tqdm import tqdm
import logging
from pathlib import Path
from functools import partial
from multiprocessing import Pool, TimeoutError

logging.basicConfig(level=logging.INFO)


def export_sdf_shard(shard_meta, filename, shard_dir, width=2):
    shard_id, item_range = shard_meta

    shard_filename = shard_dir / "{}_shard{:0{}}.sdf".format(
        filename.stem, shard_id, width
    )

    logging.info("Exporting shard `{}`".format(shard_filename))
    sdf_reader = Chem.SDMolSupplier(filename.as_posix())
    sdf_writer = Chem.SDWriter(shard_filename.as_posix())

    for idx in range(*item_range):
        mol = sdf_reader[idx]
        if mol is not None:
            sdf_writer.write(mol)

    sdf_writer.close()


def shard_sdf(filename, shard_dir, num_shards=100, num_proc=6):
    suppl = Chem.SDMolSupplier(filename.as_posix())

    logging.info(
        "Getting number of items in `{}`. This can take a while.".format(filename)
    )
    num_items = len(suppl)
    logging.info("{} items".format(num_items))

    shard_size = num_items // num_shards + 1

    if not shard_dir.exists():
        shard_dir.mkdir()

    shard_meta = [
        (shard_id, (item_id, min(item_id + shard_size, num_items)))
        for (shard_id, item_id) in zip(
            range(num_shards), range(0, num_items, shard_size)
        )
    ]

    with tqdm(total=len(shard_meta)) as progress_bar:
        with Pool(num_proc) as pool:
            it = pool.imap_unordered(
                partial(
                    export_sdf_shard,
                    filename=filename,
                    shard_dir=shard_dir,
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
        logging.info("Finished SDF sharding")


if __name__ == "__main__":
    _data_root = Path("/data/ChEMBL/chembl_25")
    chembl_sdf = _data_root / "chembl_25.sdf"
    shard_dir = _data_root / "chembl_25_shards"

    shard_sdf(chembl_sdf, shard_dir, num_shards=100)
