from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = fh.read().splitlines()

setup(
    name="dataverse-batch",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Python library for batch processing in Microsoft Dataverse",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    keywords="dataverse, dynamics, batch, microsoft, crm",
    project_urls={
        "Bug Reports": "https://github.com/tolonit/dataverse-batch/issues",
        "Source": "https://github.com/tolonit/dataverse-batch",
    },
)