from setuptools import setup, find_packages
args = dict(
    name = 'gwikibot',
    version = '0.1',
    zip_safe = False,
    packages = find_packages(),
    install_requires=[
        'sqlalchemy',
        'gevent',
        'pyyaml',
    ],
)

if __name__ == '__main__':
    setup(**args)
