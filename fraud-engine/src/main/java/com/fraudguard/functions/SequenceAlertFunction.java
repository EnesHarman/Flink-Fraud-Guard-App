package com.fraudguard.functions;

import com.fraudguard.model.Alert;
import com.fraudguard.model.Transaction;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class SequenceAlertFunction implements PatternSelectFunction<Transaction, Alert> {

    @Override
    public Alert select(Map<String, List<Transaction>> pattern) {
        Transaction firstLow = pattern.get("firstLow").get(0);
        Transaction highAmount = pattern.get("highAmount").get(0);

        String message = String.format("Sequence Fraud Detected: 2 low transactions (%.2f, %.2f) followed by a high transaction (%.2f) within 60s.",
                firstLow.getAmount(),
                pattern.get("secondLow").get(0).getAmount(),
                highAmount.getAmount());

        return new Alert(
                highAmount.getTransactionId(),
                highAmount.getUserId(),
                "CEP_SEQUENCE_FRAUD",
                message
        );
    }
}
