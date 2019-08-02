## Run Code Locally

### Set Repo Environment

Install `virtualenv` to set up the python project environment:

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

# Install repo dependencies
pip install -r requirements.txt
```

If PySpark is already installed on your machine. You can directly run the job using `spark-submit`.

### Run PySpark Job

```
spark-submit main.py \
    --payment_data=./data/acme_december_2018.csv \
    --transaction_type_data=./data/transaction_type.csv \
    --transaction_type_backend_data=./data/transaction_type_backend.csv \
    --merchant_business_type_data=./data/merchant_business_type.csv \
    --mcc_data=./data/mcc_data.csv
```

To get more details about arguments to be passed:
`spark-submit main.py --help`

## Run code using Docker
If `PySpark` is not installed, you may build a docker container with `PySpark` installed.

### Build Docker Container
```
docker build -t oms-code-challenge-image .
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
    oms-code-challenge-image \
    spark-submit main.py \
        --payment_data=acme_december_2018.csv \
        --transaction_type_data=transaction_type.csv \
        --transaction_type_backend_data=transaction_type_backend.csv \
        --merchant_business_type_data=merchant_business_type.csv \
        --mcc_data=mcc_data.csv

docker cp $(docker ps -aqf "name=omise-code-challenge-container"):/home/output/ .
```
