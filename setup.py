from setuptools import setup, find_packages

setup(
    name="cosmos-model",
    version="0.1.3",
    description="Cosmos model library to execute function in HPC",
    author="Pablo Arancibia Barahona",
    packages=find_packages(),
    include_package_data=True,
    install_requires=["python-dotenv", "paramiko"],
)
