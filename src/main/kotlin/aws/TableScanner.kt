package aws

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

interface TableScanner {
    fun getItems(exclusiveStartKey: Map<String, AttributeValue>): ScanResult;
}