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

Clone the repository into a local folder

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
