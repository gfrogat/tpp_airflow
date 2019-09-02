import subprocess
from pathlib import Path

from setuptools import find_packages, setup

cwd = Path("").absolute()

version = "0.1.0"
sha = "Unknown"

try:
    sha = (
        subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=cwd)
        .decode("ascii")
        .strip()
    )
except Exception:
    pass

version_path = cwd / "tpp" / "version.py"

with open(version_path, "w") as f:
    f.write(f'__version__ = "{version}"\n')
    f.write(f'git_version = "{sha}"')


requirements = {"install": ["setuptools", "pyspark[sql]==2.4.3"]}
install_requires = requirements["install"]

setup(
    # Metadata
    name="tpp",
    version=version,
    author="Peter Ruch",
    author_email="tpp@ml.jku.at",
    license="MIT",
    description="Target Prediction Platform",
    # Package info
    packages=find_packages(),
    install_requires=install_requires,
    zip_safe=False,
)
