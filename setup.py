from setuptools import setup, find_packages

setup(
    name="relayx_py",
    version="1.0.0",
    packages=["relayx_py"],
    install_requires=["nats-py", "pytest-asyncio"],
    author="Relay",
    description="A SDK to connect to the Relay Network",
    license="MIT"
)