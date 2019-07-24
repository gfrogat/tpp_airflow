#!/bin/env python
import argparse
import pandas as pd
import sqlite3
import os
from pathlib import Path

import logging

_chembl_path = Path("/publicdata/tpp/ChEMBL/chembl_25/chembl_25_sqlite")
_db_path = _chembl_path / "chembl_25.db"
_parquet_path = _chembl_path / "chembl_25_compounds_dump.parquet"

logging.basicConfig(level=logging.INFO)


def dump_chembl_sqlite(db_path, parquet_path):
    logging.info("Connecting to databese")
    cnx = sqlite3.connect(db_path)

    query = """
    SELECT
        md.chembl_id as mol_id,
        cs.standard_inchi as inchi,
        cs.standard_inchi_key as inchikey,
        cs.canonical_smiles as smiles
    from
        molecule_dictionary md
    join
        compound_structures cs on md.molregno = cs.molregno
    """

    logging.info("Running query")
    df = pd.read_sql(query, cnx)

    logging.info("Writing results to file")
    df.to_parquet(parquet_path)

    logging.info(
        "Successfully exported compounds `{}` to file `{}`".format(
            db_path, parquet_path
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ChEMBL sqlite Exporter")
    parser.add_argument(
        "--input",
        type=str,
        dest="db_path",
        default=_db_path,
        help="Path to ChEMBL `sqlite` database file",
    )
    parser.add_argument(
        "--output",
        type=str,
        dest="parquet_path",
        default=_parquet_path,
        help="Path where output `parquet` should be written to",
    )

    args = parser.parse_args()

    if not os.path.exists(args.db_path):
        raise ValueError("Path {} does not exist".format(_db_path))

    if not os.path.exists(os.path.dirname(args.parquet_path)):
        raise ValueError("Parent folder of parquet does not exist")

    dump_chembl_sqlite(args.db_path, args.parquet_path)
