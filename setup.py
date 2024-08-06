from setuptools import setup, find_packages

setup(
    name='bi-functions',
    version='0.1',
    packages=find_packages(),  # This should include all subpackages
    install_requires=[
        'psycopg2-binary',
        'python-dotenv',
    ],
    include_package_data=True,
    description='A library of functions for BI applications',
    author='Your Name',
    author_email='your.email@example.com',
    url='https://github.com/jgarcia-neostella/BI-Functions',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
