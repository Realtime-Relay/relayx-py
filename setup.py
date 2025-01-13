from setuptools import setup, find_packages

setup(
    name="relay",
    version="1.0.0",
    packages=["realtime"],
    install_requires=["nats-py"],
    author="Relay",
    description="A SDK to connect to the Relay Network"
)