package kcl

import Constants
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput
import java.time.Clock;
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class KclRecordProcessor(
    private val tableName: String,
    private val eventsQueue: ArrayBlockingQueue<KclRecordsWrapper>,
    private val shardRegister: ConcurrentHashMap<String, ShardInfo>,
    private val clock: Clock,
) : IRecordProcessor {

    private var shutdownRequested = false;
    private lateinit var shardId: String;
    private var lastCheckpointTime = 0L;
    private lateinit var lastProcessedSeqNum: String;

    override fun initialize(initializationInput: InitializationInput) {
        shardId = initializationInput.shardId;
        lastCheckpointTime = clock.millis();
        lastProcessedSeqNum = "";

        shardRegister.putIfAbsent(shardId, ShardInfo(initializationInput.shardId));
    }

    override fun processRecords(processRecordsInput: ProcessRecordsInput?) {
        val records = processRecordsInput?.records ?: return;

        val events = KclRecordsWrapper(shardId, records);
        var added = false;
        while (!added && !shutdownRequested) {
            added = try {
                eventsQueue.offer(events, 100, TimeUnit.MILLISECONDS)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        if (shutdownRequested) {
            // TODO: log
        }

        val firstProcessedSeqNum = records.get(0).sequenceNumber;
        val lastProcessedSeqNum = records.get(records.size - 1).sequenceNumber;
        // TODO: log first and last
    }

    fun checkpoint(checkpointer: IRecordProcessorCheckpointer) {
        if (isTimeToCheckpoint()) {
            val lastCommittedRecordSeqNum = shardRegister.get(shardId)?.getLastCommittedRecordSeqNum() ?: return;

            try {
                checkpointer.checkpoint(lastCommittedRecordSeqNum);
                lastCheckpointTime = clock.millis();
            } catch (e: IllegalArgumentException) {
                throw RuntimeException("Invalid sequence number", e);
            } catch (e: InvalidStateException) {
                throw RuntimeException("Invalid kcl state", e);
            } catch (e: ShutdownException) {
                throw RuntimeException("Failed to checkpoint", e);
            }
        }
    }

    private fun isTimeToCheckpoint(): Boolean {
        val passedTime = clock.millis() - lastCheckpointTime ;
        return TimeUnit.MILLISECONDS.toSeconds(passedTime) >= Constants.KclRecordProcessorCheckpointingInterval;
    }

    override fun shutdown(shutdownInput: ShutdownInput?) {
        shutdownRequested = true;

        try {
            when (shutdownInput?.shutdownReason) {
                ShutdownReason.TERMINATE -> onTerminate(shutdownInput);
                ShutdownReason.ZOMBIE -> onZombie();
                else -> return;
            }
        }  catch (e: InterruptedException) {
            throw RuntimeException("Thread interrupted during shutdown", e);
        } catch (e: InvalidStateException) {
            throw RuntimeException("Invalid kcl state", e);
        } catch (e: ShutdownException) {
            throw RuntimeException("Failed to checkpoint", e);
        }
    }

    private fun onTerminate(shutdownInput: ShutdownInput) {
        if (lastProcessedSeqNum.isNotEmpty()) {
            val processRegister = shardRegister.get(shardId) ?: return;
            var i = 0;
            while (!processRegister.getLastCommittedRecordSeqNum().equals(lastProcessedSeqNum)) {
                if (i % 20 == 0) {
                    // TODO log shared ended
                }
                i += 1;

                Thread.sleep(500);
            }
        }

        shardRegister.remove(shardId);

        shutdownInput.checkpointer?.checkpoint();
    }

    private fun onZombie() {
        shardRegister.remove(shardId);
    }

    fun shutdownRequested(checkpointer: IRecordProcessorCheckpointer) {
        shutdownRequested = true;

        val shardInfo = shardRegister.get(shardId) ?: return;
        if (!shardInfo.getLastCommittedRecordSeqNum().equals("")) {
            // TODO log graceful shutdown requested
        }

        try {
            checkpointer.checkpoint(shardInfo.getLastCommittedRecordSeqNum());
        } catch (e: Throwable) {
            // log failed to checkpoint at shutdown exception
        }
        shardRegister.remove(shardId);
    }
}