from setuptools import setup

setup(
    name='apache-beam-io-extras',
    version='0.0.1',
    py_modules=['beam_io_extras'],
    install_requires=[
        'apache-beam==2.5',
    ],
    url='https://github.com/invanatech/apache-beam-python-ptransforms',
    license='MIT License',
    author='rrmerugu',
    author_email='rrmerugu@gmail.com',
    description='The missing I/O Transforms in python which already exist in Java SDK based on https://beam.apache.org/documentation/io/built-in/'
)
