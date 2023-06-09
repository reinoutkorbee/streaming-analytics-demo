#!/bin/zsh

cd confluent-7.3.2

./bin/kafka-console-producer --bootstrap-server localhost:9092 --topic streaming-demo