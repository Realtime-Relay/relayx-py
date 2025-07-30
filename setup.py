from setuptools import setup, find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="relayx_py",
    version="1.0.15",
    packages=["relayx_py"],
    install_requires=["nats-py==2.10.0", "pytest-asyncio==1.0.0", "nkeys==0.2.1", "msgpack==1.1.1", "tzlocal==5.3.1"],
    author="Relay",
    description="A powerful library for integrating real-time communication into your software stack, powered by the Relay Network.",
    license="Apache 2.0",
    long_description=long_description,
    long_description_content_type="text/markdown",
)