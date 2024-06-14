from pathlib import Path
from typing import List

import setuptools

from pyspark_expand.__version__ import VERSION

# The directory containing this file
HERE = Path(__file__).parent.resolve()

# The text of the README file
NAME = "pyspark_expand"
AUTHOR = "Digital Industrial Platform"
SHORT_DESCRIPTION = (
    "Add-in for the SCIP solver with support for heuristics, "
    "classical machine learning and deep learning methods"
)
README = Path(HERE, "README.md").read_text(encoding="utf-8")
# URL = "https://gitlab.idp.yc.ziiot.ru/ds_and_math_users/components/zyopt"
URL = ""
REQUIRES_PYTHON = ">=3.8"
LICENSE = "BSD 3-Clause"


def _readlines(*names: str, **kwargs) -> List[str]:
    encoding = kwargs.get("encoding", "utf-8")
    lines = (
        Path(__file__)
        .parent.joinpath(*names)
        .read_text(encoding=encoding)
        .splitlines()
    )
    return list(map(str.strip, lines))


def _extract_requirements(file_name: str):
    return [
        line for line in _readlines(file_name) if line and not line.startswith("#")
    ]


def _get_requirements(req_name: str):
    requirements = _extract_requirements(req_name)
    return requirements


setuptools.setup(
    name=NAME,
    version=VERSION,
    author=AUTHOR,
    author_email="",
    description=SHORT_DESCRIPTION,
    long_description=README,
    long_description_content_type="text",
    url=URL,
    python_requires=REQUIRES_PYTHON,
    license=LICENSE,
    packages=setuptools.find_packages(exclude=["test*"]),
    # entry_points={
    # "console_scripts": [
    # "zyopt-cli = zyopt.cli:main",
    # ],
    # },
    include_package_data=True,
    install_requires=_get_requirements("requirements.txt"),
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
