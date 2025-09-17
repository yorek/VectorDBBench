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

Change directoies into VectorDBBench and Install the VectorDBBench and its dependencies

```
cd VectorDBBench
pip install pyodbc
pip install .
```


## Run VectorDBBench on the Command Line Interface with help

```
vectordbbench mssql --help
```

## Run VectorDBBench on the Command Line Interface
The database must exist and there must be enough room to build the index

```
vectordbbench mssql --database=vectordb --server=**IP_ADDRESS** --uid=sa --pwd=**PASSWORD_HERE**  --concurrency-duration=1800 --skip-search-concurrent --case-type=Performance1536D500K 
```

## Run VectorDBBench on the Command Line Interface with Existing Data

```
vectordbbench mssql --database=vectordb --server=**IP_ADDRESS** --uid=sa --pwd=**PASSWORD_HERE**  --concurrency-duration=1800 --skip-search-concurrent --case-type=Performance1536D500K 
```

## Start VectorDBBench in the GUI Mode
```
python -m vectordb_bench
```
