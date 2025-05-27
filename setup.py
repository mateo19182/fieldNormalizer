from setuptools import setup, find_packages

setup(
    name="ultimate-parser",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "python-dotenv",
        "requests",
        "tqdm",
        "aiohttp",
        "sqlparse"
    ],
    entry_points={
        'console_scripts': [
            'ultimate-parser=src.cli:main',
        ],
    },
    description="The ultimate tool for extracting and parsing data from various file formats",
    author="Your Name",
    author_email="your.email@example.com",
    python_requires=">=3.6",
)
