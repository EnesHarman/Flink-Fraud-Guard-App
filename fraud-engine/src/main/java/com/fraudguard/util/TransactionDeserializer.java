package com.fraudguard.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraudguard.model.Transaction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class TransactionDeserializer implements DeserializationSchema<Transaction> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Transaction deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, Transaction.class);
    }

    @Override
    public boolean isEndOfStream(Transaction nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}
