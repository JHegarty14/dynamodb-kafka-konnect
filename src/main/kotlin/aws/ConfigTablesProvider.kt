package aws

import DynamoDbSourceConnectorConfig
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import java.util.*


class ConfigTablesProvider(private val client: AmazonDynamoDB, config: DynamoDbSourceConnectorConfig) : TablesProviderBase() {
    private val tables: List<String>;

    init {
        this.tables = config.getWhitelistTables() ?: listOf();
    }

    override fun getConsumableTables(): List<String> {
        val consumableTables = LinkedList<String>();
        for (table in tables) {
            val tableDesc = try {
                client.describeTable(table).table;
            } catch (_: Throwable) {
                continue;
            }

            if (hasValidConfig(tableDesc, table)) {
                consumableTables.add(table);
            }
        }

        return consumableTables;
    }
}