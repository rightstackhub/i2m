from setuptools import setup, find_packages  # Always prefer setuptools over distutils
from codecs import open  # To use a consistent encoding
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'DESCRIPTION.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='insight2marketo',
    version='1.2.0b1',
    description='A integration between a mySQL database and Marketo, allowing for periodic and bulk updates',
    long_description=long_description,

    # Author details
    author='Ben Johnson',
    author_email='ben@rightstack.io',

    classifiers=[
        'Development Status :: 5 - Stable',

        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
    ],
    packages=find_packages()
    install_requires=['requests', 'pymysql'],

    # If there are data files included in your packages that need to be
    # installed, specify them here.  If using Python 2.6 or less, then these
    # have to be included in MANIFEST.in as well.
    package_data={
        'fieldmap': ['data/fieldmap.csv'],
    },

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points={
        'console_scripts': [
            'periodic=insight2marketo:syncPeriodic',
            'bulk=insight2marketo:syncBulk',
        ],
    },
)