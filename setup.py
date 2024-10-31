from setuptools import setup, find_packages

setup(
    name="realtime",
    version="1.0.0",
    packages=["realtime"],
    install_requires=["websocket-client"],
    author="Realtime Tango",
    description="A SDK to connect to the realtime network"
)