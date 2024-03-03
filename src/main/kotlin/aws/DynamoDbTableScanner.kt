package aws

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.model.ScanResult
import com.google.common.util.concurrent.RateLimiter

class DynamoDbTableScanner
    (private val client: AmazonDynamoDB, private val tableName: String, readCapacityUnits: Long) : TableScanner {
    private val rateLimiter: RateLimiter?;
    private var permitsToConsume = 1;

    init {
        if (readCapacityUnits > 0L) {
            // TODO
            this.rateLimiter = RateLimiter.create((readCapacityUnits / 2).toDouble());
        } else {
            this.rateLimiter = null;
        }
    }

    override fun getItems(exclusiveStartKey: Map<String, AttributeValue>): ScanResult {
        rateLimiter?.acquire(permitsToConsume)

        val scan = ScanRequest()
            .withTableName(tableName)
            .withLimit(1000)
            .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
            .withExclusiveStartKey(exclusiveStartKey);

        val result = client.scan(scan);

        if (rateLimiter != null) {
            val consumedCapacity = result.consumedCapacity.capacityUnits;
            permitsToConsume = (consumedCapacity - 1.0).toInt();
            permitsToConsume = if (permitsToConsume <= 0) 1 else permitsToConsume;
        }

        return result;
    }
}