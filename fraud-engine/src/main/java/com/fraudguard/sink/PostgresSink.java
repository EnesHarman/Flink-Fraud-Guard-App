package com.fraudguard.sink;

import com.fraudguard.model.Alert;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

public class PostgresSink {
    public static SinkFunction<Alert> createSink() {
        String sql = "INSERT INTO alerts (alert_id, user_id, alert_type, message) VALUES (?, ?, ?, ?)";

        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<Alert>() {
                    @Override
                    public void accept(PreparedStatement ps, Alert alert) throws SQLException {
                        // Map Alert fields to SQL columns
                        ps.setString(1, alert.alertId);
                        ps.setString(2, alert.userId);
                        ps.setString(3, alert.alertType);
                        ps.setString(4, alert.message);
                    }
                },
                // JDBC Execution Options (e.g., batch size)
                JdbcExecutionOptions.builder() // Use the static builder method
                        .withBatchSize(5)
                        .build(),
                // JDBC Connection Options
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder() // Use the static builder method
                        .withUrl("jdbc:postgresql://postgres:5432/fraudguard_db")
                        .withDriverName("org.postgresql.Driver")
                        .withUsername("flinkuser")
                        .withPassword("flinkpassword")
                        .build()
        );
    }
}
