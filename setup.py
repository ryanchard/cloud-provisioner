from setuptools import setup
import ggprovisioner

readme_text = ''
with open('README.md', 'r') as f:
    readme_text = f.read()

setup(
    name='ggprovisioner',
    version='0.1.0',

    install_requires=['sqlalchemy', 'psycopg2', 'boto', 'pytz'],
    packages=['ggprovisioner',
              'ggprovisioner.cloud', 'ggprovisioner.cloud.aws',
              'ggprovisioner.scheduler', 'ggprovisioner.scheduler.condor'],
    package_data={'': ['*.ini']},
    entry_points={'console_scripts':
                  ['genomics_provisioner = ggprovisioner.cli:main']},

    long_description=readme_text
)
