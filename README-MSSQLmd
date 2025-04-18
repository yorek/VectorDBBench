# Run VectorDBBench against MSSQL database

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

```
git clone https://github.com/MSSQL-VectorDBBench/VectorDBBench
```

Clone the repository into a local folder

## Install VectorDBBench dependencies

Install the VectorDBBench dependencies

```
pip install -e '.[test]'
pip install -e '.[mssql]'
```


## Run VectorDBBench with help

```
vectordbbench mssql --help
```

## Run VectorDBBench
```
vectordbbench mssql --database=vectordb --server=10.177.3.78 --uid=sa --pwd=--concurrency-duration=1800 --skip-search-concurrent --case-type=Performance1536D500K --skip-load --skip-drop-old

```

## Start the Server
```
python -m vectordb_bench
```
