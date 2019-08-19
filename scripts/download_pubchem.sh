#!/bin/env bash

PUBCHEM_DIR="/publicdata/tpp/datasets/PubChem"
PUBCHEM_BIOASSAY_URL="ftp://ftp.ncbi.nlm.nih.gov/pubchem/Bioassay/CSV/Data"
PUBCHEM_COMPOUND_URL="ftp://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF"

PUBCHEM_VERSION="pubchem_"$(date +"%Y%m%d")


if [[ -z ${PUBCHEM_DIR} ]]; then
    echo "PubChem output dir is not set";
    echo "Set PUBCHEM_DIR evironment variable and rerun script";
    echo "Example: PUBCHEM_DIR=/data bash download_pubchem.sh"
    exit
fi

wget -mP "${PUBCHEM_DIR}/${PUBCHEM_VERSION}" "${PUBCHEM_BIOASSAY_URL}"
wget -mP "${PUBCHEM_DIR}/${PUBCHEM_VERSION}" "${PUBCHEM_COMPOUND_URL}"
