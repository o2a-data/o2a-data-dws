#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [ ]

setup_requirements = [ ]

test_requirements = [ ]

setup(
    author="Ana Macario",
    author_email='ana.macario@awi.de',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: The 3-Clause BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="The o2a-data-dws modules supports on the access of O2A data services for the purpose of dowload, analysis and visualization of near real-time data.",
    entry_points={
        'console_scripts': [ ],
    },
    install_requires=requirements,
    license="Apache-2",
    long_description=readme + '\n\n',
    include_package_data=True,
    keywords='dws',
    name='o2a-data-dws',
    packages=find_packages(include=['o2a_data_dws']),
    url='https://github.com/o2a-data/o2a-data-dws.git',
    version='0.1.0',
    zip_safe=False,
)
