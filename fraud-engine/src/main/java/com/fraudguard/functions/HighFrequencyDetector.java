package com.fraudguard.functions;

import com.fraudguard.model.Alert;
import com.fraudguard.model.Transaction;
import com.fraudguard.util.OutputTags;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.stream.StreamSupport;

public class HighFrequencyDetector extends ProcessWindowFunction<Transaction, Alert, String, TimeWindow> {

    @Override
    public void process(String userId, ProcessWindowFunction<Transaction, Alert, String, TimeWindow>.Context context, Iterable<Transaction> elements, Collector<Alert> out) throws Exception {
        System.out.println("Triggered windowwwwww");

        long count = StreamSupport.stream(elements.spliterator(), false).count();
        if (count > 5) {
            Transaction first = elements.iterator().next();

            String msg = "High Frequency Alert: " + count + " transactions detected in 10 seconds.";
            out.collect(new Alert(first.getTransactionId(), userId, "HIGH_FREQUENCY", msg));
        }
    }
}
