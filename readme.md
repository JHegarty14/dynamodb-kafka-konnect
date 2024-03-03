# dynamodb-kafka-konnect

A Kotlin port of the [kafka-connect-dynamodb](https://github.com/trustpilot/kafka-connect-dynamodb/tree/master) project
which implements a [source connector](http://kafka.apache.org/documentation.html#connect) for AWS DynamoDB Streams. This
connector allows for effortless replication of data from DynamoDB tables into Kafka topics with strong consistency
guarantees, which is especially useful when building data pipelines or implementing transactional outboxes to support
asynchronous inter-service communication.

# NOT SUITED FOR PRODUCTION USE!

This was done as a way to practice Kotlin. This connector is missing test coverage, proper logging, and has not been
tested at any meaningful scale. Please use the `kafka-connect-dynamodb` project linked above.