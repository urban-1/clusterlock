
#!/usr/bin/env python
# -*- coding: utf-8 -*-


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from clusterlock import __version__


with open('README.rst') as readme_file:
    readme = readme_file.read()


setup(
    name='clusterlock',
    version=__version__,
    description="A database based locking and semaphore implementation",
    long_description=readme,
    author="Andreas Bontozoglou",
    author_email='bodozoglou@gmail.com',
    url='https://github.com/urban-1/clusterlock',
    packages=[
        'clusterlock',
    ],
    package_dir={'clusterlock': 'clusterlock'},
    include_package_data=True,
    install_requires=[
        'SQLAlchemy'
    ],
    license="WTFPL",
    keywords='distributed database lock semaphre',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
    ],
    test_suite='tests',
    #entry_points={
        #'console_scripts': [
            #'pipreqs=pipreqs.pipreqs:main',
        #],
    },
) 
