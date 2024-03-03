package aws

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder
import com.amazonaws.services.resourcegroupstaggingapi.AWSResourceGroupsTaggingAPI
import com.amazonaws.services.resourcegroupstaggingapi.AWSResourceGroupsTaggingAPIClientBuilder

object AwsClients {
    @JvmStatic
    fun buildDynamoDbClient(
        awsRegion: String,
        serviceEndpoint: String?,
        awsAccessKeyId: String?,
        awsSecretKey: String?,
    ): AmazonDynamoDB {
        return configureBuilder(
            AmazonDynamoDBClientBuilder.standard(),
            awsRegion,
            serviceEndpoint,
            awsAccessKeyId,
            awsSecretKey
        ).build();
    }

    @JvmStatic
    fun buildDynamoDbStreamsClient(
        awsRegion: String,
        serviceEndpoint: String?,
        awsAccessKeyId: String?,
        awsSecretKey: String?,
    ): AmazonDynamoDBStreams {
        return configureBuilder(
            AmazonDynamoDBStreamsClientBuilder.standard(),
            awsRegion,
            serviceEndpoint,
            awsAccessKeyId,
            awsSecretKey
        ).build();
    }

    @JvmStatic
    fun buildAwsResourceGroupsTaggingApiClient(
        awsRegion: String,
        serviceEndpoint: String?,
        awsAccessKeyId: String?,
        awsSecretKey: String?,
    ): AWSResourceGroupsTaggingAPI {
        return configureBuilder(
            AWSResourceGroupsTaggingAPIClientBuilder.standard(),
            awsRegion,
            serviceEndpoint,
            awsAccessKeyId,
            awsSecretKey
        ).build();
    }

    @JvmStatic
    fun getCredentials(awsAccessKey: String?, awsSecretKey: String?): AWSCredentialsProvider {
        if (awsAccessKey.isNullOrBlank() || awsSecretKey.isNullOrBlank()) {
            return DefaultAWSCredentialsProviderChain.getInstance();
        }

        val awsCreds = BasicAWSCredentials(awsAccessKey, awsSecretKey);
        return AWSStaticCredentialsProvider(awsCreds);
    }

    @JvmStatic
    private fun <SubClass : AwsClientBuilder<*, *>, TypeToBuild>configureBuilder(
        builder: AwsClientBuilder<SubClass, TypeToBuild>,
        awsRegion: String,
        serviceEndpoint: String?,
        awsAccessKeyId: String?,
        awsSecretKey: String?,
    ): AwsClientBuilder<SubClass, TypeToBuild> {
        builder.withCredentials(getCredentials(awsAccessKeyId, awsSecretKey))
            .withClientConfiguration(ClientConfiguration().withThrottledRetries(true));

        if (!serviceEndpoint.isNullOrEmpty()) {
            builder.withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration(serviceEndpoint, awsRegion));
        } else {
            builder.withRegion(awsRegion);
        }

        return builder;
    }
}