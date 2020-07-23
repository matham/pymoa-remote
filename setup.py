from setuptools import setup, find_packages
from io import open
from os import path

from pymoa_remote import __version__

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

URL = 'https://github.com/matham/pymoa-remote'

setup(
    name='pymoa-remote',
    version=__version__,
    author='Matthew Einhorn',
    author_email='moiein2000@gmail.com',
    license='MIT',
    description='PyMoa-Remote is a library to remotely execute functions '
                'e.g. on a Raspberry Pi.',
    long_description=long_description,
    url=URL,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',
        'License :: OSI Approved :: MIT License',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Hardware',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    packages=find_packages(),
    install_requires=['trio', 'outcome', 'tree_config'],
    extras_require={
        'dev': [
            'pytest>=3.6', 'pytest-cov', 'flake8', 'sphinx-rtd-theme',
            'coveralls', 'pytest-trio', 'sphinxcontrib-trio'],
        'network': [
            'quart', 'quart-trio', 'asks', 'trio-websocket',
            'async_generator'],
    },
    package_data={
        'pymoa_remote':
            []},
    project_urls={
        'Bug Reports': URL + '/issues',
        'Source': URL,
    },
    entry_points={
        'console_scripts':
            ['pymoa_quart_app=pymoa_remote.app.quart:run_app',
             'pymoa_multiprocessing_app=pymoa_remote.'
             'app.multiprocessing:run_app',
             ]
    },
)
