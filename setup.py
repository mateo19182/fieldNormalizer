from setuptools import setup, find_packages

setup(
    name="field-normalizer",
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
            'field-normalizer=src.cli:main',
        ],
    },
    description="A tool for extracting and normalizing headers from various data files",
    author="Your Name",
    author_email="your.email@example.com",
    python_requires=">=3.6",
)
