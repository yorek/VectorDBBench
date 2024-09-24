# Run VectorDBBench agains MSSQL database

VectorDBBench has been tested running on WSL2 + Ubuntu 22.04.4 LTS.

## Install ODBC 

Follow instructions here: https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server

## Install Python 3.11

Follow instructions here: https://ubuntuhandbook.org/index.php/2022/10/python-3-11-released-how-install-ubuntu/)

## Install pip for Python3.11 :

Use the following commands:

```
sudo apt install python3.11 python3.11-distutils python3.11-venv
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11
```

## Clone the repository

Clone the repository into a local folder

## Create Virtual Environment

In local folder where you have cloned the repository, create a virtual environment:

```
python3.11 -m venv .venv
```

then activate it:

```
. ./.venv/bin/activate
```

## Install VectorDBBench dependencies

Install the VectorDBBench dependencies

```
pip install -e '.[test]'
pip install -e '.[mssql]'
```

## Run VectorDBBench

```
python -m vectordb_bench
```

## Run VectorDBBench CLI

### Arguments
- server: host address
- database: name of database
- uid: user id
- pwd: user password
- concurency-duration: length (in seconds) to run benchmark, per user
- num-concurrency - list of users to run the benchmark for
- case-type: Benchmark to run, eg. Performance1536D500K, Performance768D1M
- skip-drop-old: Drop old or skip  [default: drop-old]
- skip-load: Load or skip  [default: load]

### Load Database
```
vectordbbench mssql --case-type=Performance1536D500K --server=localhost --database=vectordb --pwd=password --uid=user_id--concurrency-duration 500 --num-concurrency '1, 3, 5'
```

### Skip Load Database
```
vectordbbench mssql --case-type=Performance1536D500K --server=localhost --database=vectordb --pwd=password --uid=user_id --skip-load --skip-drop-old --concurrency-duration 500 --num-concurrency '1, 3, 5'
```

