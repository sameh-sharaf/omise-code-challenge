FROM thiagolcmelo/spark-debian:latest

RUN apt install python3-pip

COPY . .

RUN pip3 install -r requirements.txt