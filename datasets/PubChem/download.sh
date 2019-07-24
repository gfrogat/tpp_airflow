#!/bin/env bash

PUBCHEM_DIR="/publicdata/tpp/PubChem"
PUBCHEM_BIOASSAY_URL="ftp://ftp.ncbi.nlm.nih.gov/pubchem/Bioassay/CSV/Data"
PUBCHEM_COMPOUND_URL="ftp://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF"

PUBCHEM_VERSION="pubchem_"`date +"%Y%m%d"`

wget -mP ${PUBCHEM_DIR}/${PUBCHEM_VERSION} ${PUBCHEM_BIOASSAY_URL}
wget -mP ${PUBCHEM_DIR}/${PUBCHEM_VERSION} ${PUBCHEM_COMPOUND_URL}
