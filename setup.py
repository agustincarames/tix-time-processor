from setuptools import setup, find_packages

setup(
    name="tix-time-processing",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "jsonschema==2.6.0",
        "filelock==2.0.13",
        "inflection==0.3.1",
        "python-dateutil==2.6.0",
        "pytz==2017.2",
        "requests==2.13.0",
        "pywavelets==0.5.2",
        "scipy==0.19.0",
        "numpy==1.12.0",
        "celery==4.0.2"
    ],
    test_require=[

    ]
)