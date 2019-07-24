#!/bin/env python
import argparse
import pandas as pd
import sqlite3
import os
from pathlib import Path

import logging

_chembl_path = Path("/publicdata/tpp/ChEMBL/chembl_25/chembl_25_sqlite")
_db_path = _chembl_path / "chembl_25.db"
_parquet_path = _chembl_path / "chembl_25_assays_dump.parquet"

logging.basicConfig(level=logging.INFO)


def dump_chembl_sqlite(db_path, parquet_path):
    logging.info("Connecting to database")
    cnx = sqlite3.connect(db_path)

    query = """
    SELECT
        md.chembl_id as mol_id,
        ass.chembl_id as assay_id,
        act.standard_relation as standard_relation,
        act.standard_value as standard_value,
        act.standard_units as standard_units,
        act.standard_type as standard_type,
        act.activity_comment as activity_comment,
        act.doc_id as doc_id,
        td.tid as tid,
        tt.parent_type as parent_type,
        tt.target_type as target_type,
        ass.confidence_score as confidence_score
    FROM
        target_dictionary td
    JOIN
        target_type tt ON tt.target_type = td.target_type
    JOIN
        assays ass ON td.tid = ass.tid
    JOIN
        activities act ON ass.assay_id = act.assay_id
    JOIN
        molecule_dictionary md ON act.molregno = md.molregno;
    """

    logging.info("Running query")
    df = pd.read_sql(query, cnx)

    logging.info("Writing results to file")
    df.to_parquet(parquet_path)

    logging.info(
        "Successfully exported assays `{}` to file `{}`".format(db_path, parquet_path)
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
