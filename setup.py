import os
import sys
import re
import codecs
from setuptools import setup

if sys.version_info < (3, 6, 0):
    raise RuntimeError("ibstract requires Python 3.6 or higher")


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrc]+)'")
    init_py = os.path.join(os.path.dirname(__file__),
                           'ibstract', '__init__.py')
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        else:
            raise RuntimeError('Cannot find version in ibstract/__init__.py')


here = os.path.abspath(os.path.dirname(__file__))
with codecs.open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='ibstract',
    version=read_version(),
    description=('Asynchronous financial trading data management'),
    long_description=long_description,
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Intended Audience :: Financial and Insurance Industry',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Operating System :: POSIX',
        'Environment :: Web Environment',
        'Development Status :: 3 - Alpha',
        'Topic :: Office/Business :: Financial :: Investment',
        'Topic :: Scientific/Engineering',
    ],
    platforms=['POSIX'],
    author="Jesse Liu",
    author_email="jesseliu0@gmail.com",
    url='https://github.com/jesseliu0/ibstract',
    download_url='https://pypi.python.org/pypi/ibstract',
    license='MIT',
    packages=['ibstract'],
    include_package_data=True,
    python_requires='>=3.6.0',
    install_requires=['aiomysql>=0.0.9', 'ib_insync>=0.8.5', 'pandas>=0.20.1',
                      'SQLAlchemy>=1.1.9', 'tzlocal>=1.4'],
    keywords=('ibapi asyncio interactive brokers async algorithmic'
              'quantitative trading finance')
)
