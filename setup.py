# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 10:16:38 2024

@author: Aman Jaiswar
"""

from setuptools import setup, find_packages

setup(
    name='hclfunctions',
    version='0.1.3',
    packages=find_packages(),
    description='A simple helper functions library for HighBond API.',
    author='Aman Jaiswar',
    author_email='aman.jaiswar@satorigroup.in',
    url='https://github.com/aman-satori/HCL/blob/main/hcl.py',  # Optional
    install_requires=[
        'requests',
        'requests-futures',
        'pandas',
        'numpy',
        'urllib3',
    ],  # List any dependencies here
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6'
)
