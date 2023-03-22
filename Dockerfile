FROM python:3


RUN apt-get update
RUN apt-get install default-jdk -y
RUN wget https://downloads.apache.org/kafka/3.4.0/kafka_2.13-3.4.0.tgz
RUN tar xzf kafka_2.13-3.4.0.tgz
RUN mv kafka_2.13-3.4.0 /usr/local/kafka


WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY . .

EXPOSE 80

CMD ["uvicorn", "src.main:app", "--reload", "--host", "0.0.0.0", "--port", "80" ]