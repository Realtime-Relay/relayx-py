from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


setup(
    name="relayx_py",
    version="1.0.1",
    packages=["relayx_py"],
    install_requires=["nats-py", "pytest-asyncio"],
    author="Relay",
    description="A SDK to connect to the Relay Network",
    license="Apache 2.0",
    long_description=long_description,
)