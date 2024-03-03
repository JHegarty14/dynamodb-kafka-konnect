package aws

import com.amazonaws.services.dynamodbv2.model.StreamViewType
import com.amazonaws.services.dynamodbv2.model.TableDescription

abstract class TablesProviderBase : TablesProvider {
    protected fun hasValidConfig(tableDesc: TableDescription, tableName: String): Boolean {
        val streamSpec = tableDesc.streamSpecification;
        if (!streamSpec.isStreamEnabled) {
            return false;
        }

        val streamViewType = streamSpec.streamViewType;
        return !(!streamViewType.equals(StreamViewType.NEW_IMAGE.name)
                && !streamViewType.equals(StreamViewType.NEW_AND_OLD_IMAGES.name));
    }
}