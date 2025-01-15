from setuptools import setup, find_packages

setup(
    name="relayx-py",
    version="1.0.0",
    packages=["realtime"],
    install_requires=["nats-py", "pytest-asyncio"],
    author="Relay",
    description="A SDK to connect to the Relay Network"
)