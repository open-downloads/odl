import setuptools

setuptools.setup(
    name='OpenDownloads',
    version='0.0.1',
    description='Open Downloads',
    packages=setuptools.find_packages(),
    package_data={
        'odl': [
            'odl/data/datacenters.csv',
            'odl/data/user-agents.json',
        ]
    },
    scripts=['odl/bin/odl.py'],
    entry_points={'console_scripts': [
        'odl = odl.cmdline:execute',
    ]},
    include_package_data=True,
    install_requires=[
        'apache-beam[gcp]==2.10.0', 'numpy==1.14.5', 'ipaddress', 'arrow',
        'udatetime', 'pytricia==1.0.0', 'fastavro==0.21.24'
    ])
