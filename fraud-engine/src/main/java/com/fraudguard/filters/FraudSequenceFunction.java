package com.fraudguard.filters;

import com.fraudguard.model.Transaction;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

public class FraudSequenceFunction extends IterativeCondition<Transaction> {

    private double limit;
    private boolean lessThan;

    public FraudSequenceFunction(double amount, boolean lessThan) {
        this.limit = amount;
        this.lessThan = lessThan;
    }

    @Override
    public boolean filter(Transaction transaction, Context<Transaction> ctx) throws Exception {
        if (lessThan) {
            return transaction.getAmount() < limit;
        } else {
            return transaction.getAmount() > limit;
        }
    }
}
