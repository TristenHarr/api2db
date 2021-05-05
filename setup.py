from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / 'README.rst').read_text(encoding="utf-8")

setup(
    name="api2db",
    version="0.0.6",
    description="Python Api data collection tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/TristenHarr/api2db",
    author="Tristen Harr",
    author_email="tristenharr@gmail.com",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3"
    ],
    keywords="api, pandas, database, postgresql, mariadb, google-cloud-storage, google-cloud-bigquery",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    # packages=setuptools.find_packages(exclude=["*tests.*", "*tests"]),
    python_requires=">=3.6, <4",
    # TODO: Figure out how to package each database collector as its own module I.e. pip install api2db-mysql
    # Users shouldn't need to install everything if they don't intend to use everything
    install_requires=[
        # "pymapd==0.25.0",         # Broken, requirements unnecessarily locked in
        "dill>=0.3.3",
        "pandas>=1.1.5",
        "schedule>=1.1.0",
        "pyarrow>=3.0.0",
        "urllib3>=1.26.4",
        "requests>=2.25.1",
        "SQLAlchemy>=1.4.11",
        "SQLAlchemy-Utils>=0.37.0",
        "google-cloud-bigquery>=2.11.0",
        "pandas-gbq>=0.14.1"
    ],
    extras_require={
        "postgresql": ["psycopg2>=2.8.6"],
        "mariadb": ["mariadb>=1.0.6"],
        "mysql": ["pymysql>=1.0.2"]
    },
    entry_points={
        "console_scripts": [
            "pmake=api2db.install.project_make:_pmake",
            "pclear=api2db.install.project_clear:_pclear",
            "cadd=api2db.install.collector_add:_cadd",
            "crem=api2db.install.collector_remove:_crem",
            "clist=api2db.install.collector_list:_clist",
            "mlab=api2db.install.make_lab:mlab",
            "rlab=api2db.install.run_lab:rlab",
            "clab=api2db.install.clear_lab:clab"
        ]
    },
    include_package_data=True
)
