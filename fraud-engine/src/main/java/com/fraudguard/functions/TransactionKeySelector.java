package com.fraudguard.functions;

import com.fraudguard.model.Transaction;

import  org.apache.flink.api.java.functions.KeySelector;

public class TransactionKeySelector implements KeySelector<Transaction, String> {

    @Override
    public String getKey(Transaction transaction) {
        return transaction.getUserId();
    }
}
