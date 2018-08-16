from setuptools import setup, find_packages

setup(name='scyllabackup',
      version='0.0.2',
      description='Scyllabackup: A tool for taking scylla backups',
      url='',
      license='Proprietary',
      packages=find_packages(),
      install_requires=['spongeblob==0.0.3',
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
