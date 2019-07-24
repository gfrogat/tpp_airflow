#!/bin/env bash

CHEMBL_DIR="/publicdata/tpp/ChEMBL"

# Templates for url variables
SQLITE_URL_TEMPLATE="CHEMBL_RELEASE_SQLITE_URL"
SDF_URL_TEMPLATE="CHEMBL_RELEASE_SDF_URL"

# Release 25
CHEMBL_25_SQLITE_URL="ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_25/chembl_25_sqlite.tar.gz"
CHEMBL_25_SDF_URL="ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_25/chembl_25.sdf.gz"

# Release 24.1
CHEMBL_24_SQLITE_URL="ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_24_1/chembl_24_1_sqlite.tar.gz"
CHEMBL_24_SDF_URL="ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_24_1/chembl_24_1.sdf.gz"

# Release 23
CHEMBL_23_SQLITE_URL="ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_23/chembl_23_sqlite.tar.gz"
CHEMBL_23_SDF_URL="ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_23/chembl_23.sdf.gz"

# Release 22.1
CHEMBL_22_SQLITE_URL="ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_22_1/chembl_22_1_sqlite.tar.gz"
CHEMBL_22_SDF_URL="ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_22_1/chembl_22_1.sdf.gz"

# Release 21
CHEMBL_21_SQLITE_URL="ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_21/chembl_21_sqlite.tar.gz"
CHEMBL_21_SDF_URL="ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_21/chembl_21.sdf.gz"

if [ -z ${RELEASE} ]; then
    echo "ChEMBL release is not set";
    echo "Using default release 25";
    RELEASE=25
else
    echo "ChEMBL release is set to ${RELEASE}";
fi

CHEMBL_SQLITE_URL=${SQLITE_URL_TEMPLATE/RELEASE/${RELEASE}}
CHEMBL_SDF_URL=${SDF_URL_TEMPLATE/RELEASE/${RELEASE}}

CHEMBL_RELEASE="chembl_"${RELEASE}

wget -P ${CHEMBL_DIR}/${CHEMBL_RELEASE} ${!CHEMBL_SQLITE_URL}
wget -P ${CHEMBL_DIR}/${CHEMBL_RELEASE} ${!CHEMBL_SDF_URL}