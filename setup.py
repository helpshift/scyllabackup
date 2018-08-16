from setuptools import setup, find_packages
from os import path

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md')) as f:
    long_description = f.read()

setup(name='scyllabackup',
      version='0.1.0',
      description='Scyllabackup: A tool for taking scylla backups',
      url='https://github.com/helpshift/scyllabackup',
      long_description=long_description,
      long_description_content_type='text/markdown',
      license='MIT License',
      packages=find_packages(),
      install_requires=['spongeblob==0.1.1',
                        'tenacity==4.10.0',
                        'sh==1.12.14',
                        'gevent==1.2.2',
                        'ConfigArgParse==0.13.0',
                        'filelock==3.0.4'
                        ],
      tests_require=['pytest'],
      test_suite='pytest',
      entry_points={
          'console_scripts': [
              'scyllabackup = scyllabackup.cli:cli_run_with_lock',
              'scyllabackup_migrate = scyllabackup.migrate:main'
          ]
      })
