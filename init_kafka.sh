#!/bin/zsh

wget https://packages.confluent.io/archive/7.3/confluent-community-7.3.2.tar.gz

tar -xf confluent-community-7.3.2.tar.gz

cd confluent-7.3.2

./bin/kafka-storage format --config ./etc/kafka/kraft/server.properties --cluster-id $(./bin/kafka-storage random-uuid)
