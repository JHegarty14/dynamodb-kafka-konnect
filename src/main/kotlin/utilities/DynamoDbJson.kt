package utilities

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder

object DynamoDbJson {
    val LogicalName = "io.jhegarty14.connector.dynamodb.json";

    fun schema(): Schema {
        return SchemaBuilder.string()
            .name(LogicalName)
            .version(1).build();
    }
}