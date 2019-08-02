## Run Code Locally

```
spark-submit main.py \
    --payment_data=./data/acme_december_2018.csv \
    --transaction_type_data=./data/transaction_type.csv \
    --transaction_type_backend_data=./data/transaction_type_backend.csv \
    --merchant_business_type_data=./data/merchant_business_type.csv \
    --mcc_data=./data/mcc_data.csv
```

## Run code using Docker

### Build Docker Container
```
docker build -t oms-challenge .
```

### Run Code
```
docker run -it \
    --user=root \
    --volume="$(pwd)/data/acme_december_2018.csv:/home/acme_december_2018.csv:ro" \
    --volume="$(pwd)/data/transaction_type.csv:/home/transaction_type.csv:ro" \
    --volume="$(pwd)/data/transaction_type_backend.csv:/home/transaction_type_backend.csv:ro" \
    --volume="$(pwd)/data/merchant_business_type.csv:/home/merchant_business_type.csv:ro" \
    --volume="$(pwd)/data/mcc_data.csv:/home/mcc_data.csv:ro" \
    oms-challenge \
    spark-submit main.py \
        --payment_data=acme_december_2018.csv \
        --transaction_type_data=transaction_type.csv \
        --transaction_type_backend_data=transaction_type_backend.csv \
        --merchant_business_type_data=merchant_business_type.csv \
        --mcc_data=mcc_data.csv \
    && sudo docker ps
```
