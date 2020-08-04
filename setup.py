import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="rejustify",
    version="1.0.1",
    author="M. Wolski",
    author_email="marcin@rejustify.com",
    description="Support for Rejustify API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rejustify/python-module",
    packages=setuptools.find_packages(),
    keywords='rejustify, ETL, data, economics, time-series',
    install_requires=[
        'requests >= 2.18.4',
        'pandas >= 0.21',
        'copy',
        'os',
        'json'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: GPL-3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
