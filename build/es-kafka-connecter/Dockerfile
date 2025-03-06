FROM confluentinc/cp-kafka-connect:7.0.0-1-ubi8

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:11.1.3

ADD ./jars /usr/share/java/my_transforms

ADD ./kafka_connect_transforms/src/main/java/io/mosip/kafka/connect/transforms/serviceType.json /app/config/serviceType.json
