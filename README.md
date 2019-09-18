# tpp_python

This repository contains the code for downloading various chemical datasets, extracting and cleaning (compound, assay, activity) triples, merging the datasets (i.e. combining different assay outputs per compound) and computing various chemical descriptors via [RDKit](https://github.com/rdkit/rdkit) and [Mordred Descriptor](https://github.com/mordred-descriptor/mordred).

Datasets currently supported are:

- [ChEMBL](https://www.ebi.ac.uk/chembl/)
- [PubChem](https://pubchem.ncbi.nlm.nih.gov/)
- [ZINC15](http://zinc15.docking.org/)

## Installation

Install mordred and rdkit via conda

```bash
conda install -c rdkit -c mordred-descriptor mordred
```

You can also create the environment from `environment.yml`:

```bash
conda env create -f tools/conda/environment.yml
```

`tpp_python` can be installed locally via `pip`:

```bash
pip install .
```

### Pyspark

The package makes heavy usage of Pyspark. You'll need to install it on your machine.

Download [SDKMan!](https://sdkman.io/) and install Java 8, Scala and Spark.

```bash
sdk install java 8.0.202-amzn   # optionally update to latest version
sdk install scala 2.11.12
sdk install spark 2.4.3
```

To speed things up you should also install Hadoop libraries. Download the `2.7.\*` Release from the [Website](https://hadoop.apache.org/releases.html) and add the Path to your `.bashrc` files to make Spark able to find it.

```bash
# Update to your installation location and add to your .bashrc
export HADOOP_HOME="${HOME}/Frameworks/hadoop-2.7.7"
export LD_LIBRARY_PATH="${HADOOP_HOME}/lib/native":${LD_LIBRARY_PATH}
```

Reload your `.bashrc` (or just open a new terminal session). You should now be able to run Pyspark by typing `pyspark` in the terminal.
