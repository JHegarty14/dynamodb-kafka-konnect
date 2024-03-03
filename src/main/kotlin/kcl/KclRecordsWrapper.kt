package kcl

import com.amazonaws.services.kinesis.model.Record

class KclRecordsWrapper(private val shardId: String, private val records: List<Record>) {
    fun getShardId(): String {
        return shardId;
    }

    fun getRecords(): List<Record> {
        return records;
    }
}