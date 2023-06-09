#!/bin/zsh

cd confluent-7.3.2

./bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic streaming-demo

./bin/kafka-topics --bootstrap-server localhost:9092 --create --topic streaming-demo

./bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic streaming-demo --from-beginning