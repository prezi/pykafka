#!/usr/bin/env python

import setuptools

setuptools.setup(
    name='pykafka',
    version='2.4.3',
    license='MIT',
    description=open('README.md').read(),
    author="Datadog, Inc.",
    author_email="packages@datadoghq.com",
    url='https://github.com/datadog/pykafka',
    platforms='any',
    packages=['kafka'],
    install_requires={'kazoo': ['>= 0.5', '< 0.6']},
    zip_safe=True,
    verbose=False,
)
