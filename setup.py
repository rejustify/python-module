import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="rejustify",
    version="1.0.3",
    author="M. Wolski",
    author_email="marcin@rejustify.com",
    description="Support for Rejustify API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rejustify/python-module",
    download_url='https://github.com/rejustify/python-module/archive/1.0.3.tar.gz',
    packages=setuptools.find_packages(),
    keywords=['rejustify, ETL, data, economics, time-series'],
    install_requires=[
        'requests >= 2.18.4',
        'pandas >= 0.21'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
