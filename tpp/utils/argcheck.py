import argparse
from pathlib import Path


def check_arguments(args: argparse.Namespace, dataset_name: str):
    if vars(args)[f"merge_{dataset_name}"] is True:
        if f"{dataset_name}_compounds_path" not in vars(args):
            raise ValueError(
                f"Option `--merge-chembl` requires `--{dataset_name}-compounds` to be set"
            )
        if f"{dataset_name}_assays_path" not in vars(args):
            raise ValueError(
                f"Option `--merge-chembl` requires `--{dataset_name}-assays` to be set"
            )

        compounds_path = vars(args)[f"{dataset_name}_compounds_path"]
        assays_path = vars(args)[f"{dataset_name}_assays_path"]

        if not compounds_path.exists():
            raise FileNotFoundError(f"Path {compounds_path} does not exist")

        if not assays_path.exists():
            raise FileNotFoundError(f"Path {assays_path} does not exist")


def check_arguments_chembl(args: argparse.Namespace):
    check_arguments(args, "chembl")


def check_arguments_pubchem(args: argparse.Namespace):
    check_arguments(args, "pubchem")


def check_arguments_zinc15(args: argparse.Namespace):
    if args.merge_zinc15 is True:
        if "zinc15_data_path" not in vars(args):
            raise ValueError(
                "Option `--merge-zinc15` requires `--zinc15-data` to be set"
            )

        if not args.zinc15_data_path.exists():
            raise FileNotFoundError(f"Path {args.zinc15_data_path} does not exist")


def check_path(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Path {path} does not exist")


def check_input_path(path: Path):
    check_path(path)


def check_parent_folder(path: Path):
    if not path.parent.exists():
        raise FileNotFoundError(f"Parent folder of {path} does not exist!")


def check_output_path(path: Path):
    check_parent_folder(path)

    if path.exists():
        raise FileExistsError(f"Path {path} already exists!")
