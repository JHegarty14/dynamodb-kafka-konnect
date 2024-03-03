package kcl

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import java.time.Clock
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap

class KclRecordProcessorFactory(
    private val tableName: String,
    private val eventsQueue: ArrayBlockingQueue<KclRecordsWrapper>,
    private val shardRegister: ConcurrentHashMap<String, ShardInfo>,
) : IRecordProcessorFactory {
    override fun createProcessor(): IRecordProcessor {
        return KclRecordProcessor(tableName, eventsQueue, shardRegister, Clock.systemUTC());
    }
}