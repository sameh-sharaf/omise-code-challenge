# OMISE Code Challenge

This project is done for the code challenge URLHERE. The project is written using `Python 3.6` & `PySpark 2.3.1`.  

## How To Run

### Close The Repo

Clone from GitHub using HTTPS:

`git clone https://github.com/sameh-sharaf/omise-code-challenge.git`

### Set Repo Environment

Install `virtualenv` to set up the python project environment (Optional):

```
pip install virtualenvwrapper

# Add the following to /etc/profile:
export WORKON_HOME=~/virtualenvs
source `which virtualenvwrapper.sh`

# Update env
source /etc/profile

pip install --upgrade virtualenv

# Create environment
mkvirtualenv --python=`which python3` omise-code-challenge
```

### Install Repo Dependencies

`pip install -r requirements.txt`

### Run PySpark Job

If `PySpark` is already installed on your machine. You can directly run the job using `spark-submit`.

```
spark-submit main.py \
    --payment_data=./data/acme_december_2018.csv \
    --transaction_type_data=./data/transaction_type.csv \
    --transaction_type_backend_data=./data/transaction_type_backend.csv \
    --merchant_business_type_data=./data/merchant_business_type.csv \
    --mcc_data=./data/mcc_data.csv
```

To get more details about arguments to be passed:
```
spark-submit main.py --help
```

## Run Code Using Docker
If `PySpark` is not installed, you may build a docker container with `PySpark` installed.

### Build Docker Container
```
docker build -t omise-code-challenge-image .
```

### Run Job
In order to run the job, you need to copy the required data tables to docker container.
```
docker run -it \
    --name=omise-code-challenge-container \
    --user=root \
    --volume="$(pwd)/data/acme_december_2018.csv:/home/acme_december_2018.csv:ro" \
    --volume="$(pwd)/data/transaction_type.csv:/home/transaction_type.csv:ro" \
    --volume="$(pwd)/data/transaction_type_backend.csv:/home/transaction_type_backend.csv:ro" \
    --volume="$(pwd)/data/merchant_business_type.csv:/home/merchant_business_type.csv:ro" \
    --volume="$(pwd)/data/mcc_data.csv:/home/mcc_data.csv:ro" \
    omise-code-challenge-image \
    spark-submit main.py \
        --payment_data=acme_december_2018.csv \
        --transaction_type_data=transaction_type.csv \
        --transaction_type_backend_data=transaction_type_backend.csv \
        --merchant_business_type_data=merchant_business_type.csv \
        --mcc_data=mcc_data.csv

# Get job output
docker cp $(docker ps -aqf "name=omise-code-challenge-container"):/home/output/ .
```

## Tools Used
- Python 3.6
- PySpark 2.3.1
- Pandas
- Docker 19.03.1
- Virtualenv 16.4.3

## Run The Notebook On Cloud
The `PySpark` code in this repo is available on cloud using Databricks community edition. You can access and run the notebook [here](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5087274976005126/1221193323574810/6106487312754722/latest.html).
