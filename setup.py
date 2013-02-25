from setuptools import setup
import os

setup(
    name = 'dwarf',
    version = '0.1',
    url = 'https://github.com/Camelsky/dwarf.git',
    license = 'MIT',
    author = 'camelsky',
    author_email = 'camelsky@gmail.com',
    description = 'tools for stat active user',
    zip_safe = False,
    platforms = 'any',
    package_dir = {'': '.'},
    include_package_data = True,
)