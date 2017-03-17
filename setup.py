from setuptools import setup, find_packages

setup(
    name="tix-time-processing",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "PyWavelets==0.5.2",
        "scipy==0.19.0",
        "numpy==1.12.0"
    ],
    test_require=[

    ]
)