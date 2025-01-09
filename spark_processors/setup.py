from setuptools import setup, find_packages

setup(
    name="spark_processors",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.0.0",
        "pandas>=1.0.0",
        "numpy>=1.19.0",
        "pyspark[sql]>=3.0.0"
    ],
    package_data={
        'spark_processors': ['base_schema.json'],
    },
    python_requires='>=3.9',
) 