from setuptools import setup, find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="relayx_py",
    version="1.0.7",
    packages=["relayx_py"],
    install_requires=["nats-py", "pytest-asyncio", "nkeys", "msgpack"],
    author="Relay",
    description="A SDK to connect to the Relay Network",
    license="Apache 2.0",
    long_description=long_description,
    long_description_content_type="text/markdown",
)