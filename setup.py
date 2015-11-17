from setuptools import setup, find_packages
import os

DESCRIPTION = "A python package for making it possible to do sharding inside"\
    " your Python code"

LONG_DESCRIPTION = None
try:
    LONG_DESCRIPTION = open('README.rst').read()
except:
    pass

def get_version(version_tuple):
    version = '%s.%s' % (version_tuple[0], version_tuple[1])
    if version_tuple[2]:
        version = '%s.%s' % (version, version_tuple[2])
    return version

# Dirty hack to get version number from shardmonster/__init__.py - we can't
# import it as it depends on PyMongo and PyMongo isn't installed until this
# file is read
init = os.path.join(os.path.dirname(__file__), 'shardmonster', '__init__.py')
version_line = filter(lambda l: l.startswith('VERSION'), open(init))[0]
VERSION = get_version(eval(version_line.split('=')[-1]))

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Database',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

setup(name='shardmonster',
      version=VERSION,
      packages=find_packages(),
      author='Colin Howe',
      author_email='colinthehowe@{nospam}gmail.com',
      maintainer="Colin Howe",
      maintainer_email="colinthehowe@{nospam}gmail.com",
      url='',
      license='MIT',
      include_package_data=True,
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      platforms=['any'],
      classifiers=CLASSIFIERS,
      install_requires=['pymongo>=3.1'],
      test_suite='shardmonster.tests',
      tests_require=['nose>=1.2']
)
