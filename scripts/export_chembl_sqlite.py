#!/bin/env python
import argparse
import logging
import sqlite3
from enum import Enum
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO)


class ExportType(Enum):
    ASSAY = "assays"
    COMPOUND = "compounds"

    def __str__(self):
        return self.value


assay_export_query = """
    SELECT
        md.chembl_id AS mol_id,
        ass.chembl_id AS assay_id,
        act.standard_relation AS standard_relation,
        act.standard_value AS standard_value,
        act.standard_units AS standard_units,
        act.standard_type AS standard_type,
        act.activity_comment AS activity_comment,
        act.doc_id AS doc_id,
        td.tid AS tid,
        tt.parent_type AS parent_type,
        tt.target_type AS target_type,
        ass.confidence_score AS confidence_score
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

compound_export_query = """
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


def get_query(export_type: ExportType):
    if export_type == ExportType.ASSAY:
        return assay_export_query
    elif export_type == ExportType.COMPOUND:
        return compound_export_query


def export_chembl_sqlite(db_path: Path, parquet_path: Path,
                         export_type: ExportType):
    logging.info("Connecting to databese")
    cnx = sqlite3.connect(db_path.as_posix())

    query = get_query(export_type)

    logging.info("Running query")
    df = pd.read_sql(query, cnx)

    logging.info("Writing results to file")
    df.to_parquet(parquet_path.as_posix())

    logging.info((f"Successfully exported {export_type} `{db_path}`"
                  f"to file `{parquet_path}`"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="ChEMBL SQLite Exporter",
        description=("Export {assays, compounds} from ChEMBL SQLite"
                     "database in `parquet` format."))
    parser.add_argument(
        "--input",
        type=Path,
        metavar="PATH",
        dest="db_path",
        help=f"Path to ChEMBL SQLite database",
    )
    parser.add_argument(
        "--output",
        type=Path,
        metavar="PATH",
        dest="parquet_path",
        required=True,
        help="Path where output should be written to in `parquet` format",
    )
    parser.add_argument(
        "--export",
        required=True,
        type=ExportType,
        dest="export_type",
        choices=list(ExportType)
    )

    args = parser.parse_args()

    if not args.db_path.exists():
        raise FileNotFoundError(f"Path {args.db_path} does not exist")

    if not args.parquet_path.parent.exists():
        raise FileNotFoundError(
            f"Parent folder {args.parquet_path.parent} does not exist!")

    if args.parquet_path.exists():
        raise FileExistsError(f"{args.parquet_path} already exists!")

    export_chembl_sqlite(args.db_path, args.parquet_path, args.export_type)
