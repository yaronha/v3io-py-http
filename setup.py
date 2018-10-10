# Copyright 2018 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def version():
    with open('v3io.py') as fp:
        for line in fp:
            if '__version__' in line:
                _, version = line.split('=')
                return version.replace("'", '').strip()


setup(
    name='v3io',
    version=version(),
    description='Client for v3io web API',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Yaron Haviv',
    author_email='yaronh@iguazio.com',
    license='MIT',
    url='https://github.com/yaronha/v3iopy',
    py_modules=['v3io'],
    install_requires=['requests'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries',
    ],
    tests_require=[],
)
