#!/usr/bin/env python
import os
import sys

from setuptools import find_packages

try:
    from setuptools import setup
    from setuptools.command.test import test as TestCommand

    class PyTest(TestCommand):
        def finalize_options(self):
            TestCommand.finalize_options(self)
            self.test_args = []
            self.test_suite = True

        def run_tests(self):
            # import here, because outside the eggs aren't loaded
            import pytest
            errno = pytest.main(self.test_args)
            sys.exit(errno)

except ImportError:

    from distutils.core import setup
    PyTest = lambda x: x

f = open(os.path.join(os.path.dirname(__file__), 'README.rst'))
long_description = f.read()
f.close()

setup(
    name='niceredis',
    version='0.0.1',
    description='Nice extensions for redis-py',
    long_description=long_description,
    url='http://github.com/katakumpo/niceredis',
    author='Mathias Seidler',
    author_email='seishin@gmail.com',
    maintainer='Mathias Seidler',
    maintainer_email='seishin@gmail.com',
    keywords=['Redis', 'niceredis', 'key-value store'],
    license='MIT',
    packages=find_packages(),
    install_requires=['redis'],
    tests_require=['pytest>=2.5.0', 'pytest-cov>=1.8.0'],
    zip_safe=False,
    cmdclass={'test': PyTest},
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ]
)
