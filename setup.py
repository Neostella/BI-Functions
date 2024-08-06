from setuptools import setup, find_packages

setup(
    name='bi-functions',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'psycopg2',
        'python-dotenv',
    ],
    entry_points={
        'console_scripts': [
            'get_all_views=bi_functions.db_utils.get_all_views:get_all_views',
        ],
    },
)