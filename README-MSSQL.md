# Run VectorDBBench against MSSQL database

VectorDBBench has been tested running on WSL2 + Ubuntu 22.04.4 LTS.

## Install ODBC 

Follow instructions here: https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server

## Install Python 3.11

Follow instructions here: https://ubuntuhandbook.org/index.php/2022/10/python-3-11-released-how-install-ubuntu/)

## Install pip for Python3.11 :

Use the following commands:

```bash
sudo apt install python3.11 python3.11-distutils python3.11-venv
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11
```

## Clone the repository

Clone the repository into a local folder

```bash
git clone https://github.com/yorek/VectorDBBench
```

and then move into the folder

```bash
cd VectorDBBench
```

## Create Virtual Environment

In local folder where you have cloned the repository, create a virtual environment:

```bash
python3.11 -m venv .venv
```

then activate it:

```bash
. ./.venv/bin/activate
```

## Install VectorDBBench dependencies

Change directories into VectorDBBench and install VectorDBBench and its dependencies.

You can install all dependencies with:

```bash
pip install -e '.[test]'
pip install -e '.[mssql]'
```

## Run VectorDBBench 

### Start VectorDBBench in the GUI Mode

```bash
python -m vectordb_bench
```

### Run VectorDBBench on the Command Line Interface

Get help:

```bash
vectordbbench mssql --help
```

Run a test:

```bash
vectordbbench mssql --database=vectordb --server=**IP_ADDRESS** --uid=sa --pwd=**PASSWORD_HERE**  --concurrency-duration=1800 --skip-search-concurrent --case-type=Performance1536D500K 
```
