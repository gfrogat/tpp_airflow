#!/bin/env python
import argparse
import logging
import sqlite3
from pathlib import Path

import pandas as pd

_chembl_path = Path("/publicdata/tpp/datasets/ChEMBL/chembl_25")
_db_path = _chembl_path / "chembl_25_sqlite/chembl_25.db"
_parquet_path = _chembl_path / "chembl_25_compounds_dump.parquet"

logging.basicConfig(level=logging.INFO)


def dump_chembl_sqlite(db_path: Path, parquet_path: Path):
    logging.info("Connecting to databese")
    cnx = sqlite3.connect(db_path.as_posix())

    query = """
    SELECT
        md.chembl_id AS mol_id,
        cs.standard_inchi AS inchi,
        cs.standard_inchi_key AS inchikey,
        cs.canonical_smiles AS smiles
    FROM 
        molecule_dictionary md
    JOIN
        compound_structures cs ON md.molregno = cs.molregno
    """

    logging.info("Running query")
    df = pd.read_sql(query, cnx)

    logging.info("Writing results to file")
    df.to_parquet(parquet_path.as_posix())

    logging.info(
        f"Successfully exported compounds `{db_path}` to file `{parquet_path}`"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ChEMBL SQLite Exporter")
    parser.add_argument(
        "--input",
        type=Path,
        dest="db_path",
        default=_db_path,
        help="Path to ChEMBL `sqlite` database file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        dest="parquet_path",
        default=_parquet_path,
        help="Path where output `parquet` should be written to",
    )

    args = parser.parse_args()

    if not args.db_path.exists():
        raise ValueError(f"Path {args.db_path} does not exist")

    if not args.parquet_path.parent.exists():
        raise ValueError(f"{args.parquet_path.parent} does not exist!")

    dump_chembl_sqlite(args.db_path, args.parquet_path)
