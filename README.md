# SnowIce 23 Demo Project: Streaming Analytics in Cybersecurity

## Set up the project

1. Checkout streaming-analytics-demo:

git clone git@github.com:streaming-analytics-demo.git

2. Import streaming-analytics-demo in IntelliJ

3. IntelliJ should report that it found gradle projects. Load the projects.

4. Run the following SQL code in your Snowflake DB:

`
create database streaming_demo;
`

`
create or replace table events (timestamp timestamp, value numeric);
`

`
create or replace table alerts (timestamp timestamp, value numeric, alert string);
`

On the command line execute to copy the public key:
`
pbcopy <  app/src/main/resources/ssh_keys/rsa_key.pub
`

// Remove the header and footer of the key.
`
alter user <user> set rsa_public_key='<PUBLIC KEY GOES hERE>';
`

5. Use public keys from the ssh-keys folder or generate new keys
<<<THESE KEYS ARE NOT SAFE FOR PRODUCTION, ONLY FOR TESTING AND DEVELOPMENT>>>

## Set up Kafka

Several shell scripts are available to set up Kafka.

You can either run them or look at the commands and use them manually.

Download and install Kafka Confluent:
`./init_kafka.sh`

Start the Kafka server:
`./start_kafka.sh`

Create the topic:
`./init_topic.sh`

The next time you only need to run:
`./start_kafka.sh`

If you run `./init_topic.sh` then the topic will be re-created and the consumer will be started. The produced messages are writen to the terminal.

You can test the Kafka topic with the producer:
`./start_producer.sh`

## Test Kafka

Run `TestKafka.java`

## Test JDBC

Run `TestJdbc.java`

## Run the demo

Open `ConnectionHelper` and set: ACCOUNT_NAME, REGION, USERNAME

1. Start the Kafka server: `./start_kafka.sh`
2. Initialise the topic: `./init_topic.sh`
3. Run the Ingester
4. Run the Alerter
5. Run the Producer

The data should be available in the Snowflake account.
