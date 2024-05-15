from setuptools import find_packages, setup

setup(
    name="log_elt_pipeline",
    packages=find_packages(exclude=["log_elt_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
