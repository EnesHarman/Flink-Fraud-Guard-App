package com.fraudguard.timing;

import com.fraudguard.model.Transaction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

public class TransactionTimeAssigner implements SerializableTimestampAssigner<Transaction> {

    @Override
    public long extractTimestamp(Transaction element, long recordTimestamp) {
        return element.getTimestamp();
    }

}
