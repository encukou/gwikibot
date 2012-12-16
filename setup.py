from setuptools import setup, find_packages
setup(
    name = 'wikibot',
    version = '0.1',
    zip_safe = False,
    packages = find_packages(),
    install_requires=[
        'sqlalchemy',
        'gevent',
        'pyyaml',
    ],
)
