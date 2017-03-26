from setuptools import setup, find_packages

setup(
    name="tix-time-processing",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "pywavelets==0.5.2",
        "scipy==0.19.0",
        "numpy==1.12.0",
        "apscheduler==3.3.1",
        "celery==4.0.2"
    ],
    test_require=[

    ]
)