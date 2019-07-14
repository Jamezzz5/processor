from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='Processor',
    version='0.1.0',
    author='J',
    author_email='j',
    description='Processor.',
    long_description=open('README.md').read(),
    install_requires=requirements,
)