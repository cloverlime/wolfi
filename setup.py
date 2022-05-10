#!/usr/bin/env python
from setuptools import find_packages, setup

setup(
    name="wolfi",
    version="1.0",
    package_dir={"": "src"},
    packages=find_packages("src", exclude=["*tests*"]),
    zip_safe=False,  # This is to make mypy work
)
