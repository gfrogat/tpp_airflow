from rdkit import Chem
from tqdm import tqdm

import logging
import os
from pathlib import Path

logging.basicConfig(level=logging.DEBUG)


def export_sdf_shard(shard_filename, suppl, num_items, verbose=True):
    logging.info("Exporting shard `{}`".format(shard_filename))
    sdf_writer = Chem.SDWriter(str(shard_filename))

    if verbose is True:
        suppl = tqdm(suppl, total=num_items)

    for i, mol in enumerate(suppl):
        if mol is not None:
            sdf_writer.write(mol)

        if i == num_items - 1:
            break

    sdf_writer.close()


def shard_sdf(sdf_file, out_dir, num_shards=100, verbose=True):
    suppl = Chem.SDMolSupplier(sdf_file)

    logging.info(
        "Getting number of items in `{}`. This can take a while.".format(sdf_file)
    )
    num_items = len(suppl)
    logging.info("{} items".format(num_items))

    shard_size = num_items // num_shards + 1

    out_dir = Path(out_dir)
    basename = os.path.basename(sdf_file)
    shard_basename = os.path.splitext(basename)[0]

    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    for shard_id in range(num_shards):
        shard_filename = out_dir / "{basename}_shard{id:0{width}}.sdf".format(
            basename=shard_basename, id=shard_id, width=2
        )
        export_sdf_shard(shard_filename, suppl, shard_size, verbose)


if __name__ == "__main__":
    chembl_sdf = "/local00/bioinf/tpp/chembl_25.sdf"
    shard_sdf(chembl_sdf, out_dir="chembl_25_shards", verbose=True)
