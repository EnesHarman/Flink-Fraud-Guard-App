package com.fraudguard.functions;

import com.fraudguard.model.Alert;
import com.fraudguard.model.Transaction;
import com.fraudguard.util.OutputTags;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class LocationDetectionFunction extends KeyedProcessFunction<String, Transaction, Alert> {

    private ValueState<Transaction> lastTransaction;

    @Override
    public void open(OpenContext context) throws Exception {
        lastTransaction = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTx", Transaction.class));
    }

    @Override
    public void processElement(Transaction currentTx, KeyedProcessFunction<String, Transaction, Alert>.Context ctx, Collector<Alert> out) throws Exception {
        Transaction prevTx = lastTransaction.value();
        if (prevTx != null) {
            double distance = getDistance(prevTx.getLat(), prevTx.getLon(), currentTx.getLat(), currentTx.getLon());

            double timeDiff = (double) (currentTx.getTimestamp() - prevTx.getTimestamp()) / (1000 * 60 * 60);

            if (timeDiff > 0) {
                double speed = distance / timeDiff; // km/h
                // C. The Rule: If speed > 800 km/h (approx plane speed), it's suspicious
                if (speed > 800) {
                    String msg = String.format("Impossible Travel: %.2f km in %.2f hours (Speed: %.2f km/h)",
                            distance, timeDiff, speed);

                    out.collect(new Alert(currentTx.getTransactionId(), currentTx.getUserId(), "IMPOSSIBLE_TRAVEL", msg));
                }
            }
        }
        lastTransaction.update(currentTx);
    }

    private double getDistance(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371; // Radius of the earth in km
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }




}
