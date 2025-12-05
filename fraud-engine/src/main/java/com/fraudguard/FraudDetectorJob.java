/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.fraudguard;

import com.fraudguard.filters.FraudSequenceFunction;
import com.fraudguard.functions.HighFrequencyDetector;
import com.fraudguard.functions.LocationDetectionFunction;
import com.fraudguard.functions.SequenceAlertFunction;
import com.fraudguard.functions.TransactionKeySelector;
import com.fraudguard.model.Alert;
import com.fraudguard.model.Transaction;
import com.fraudguard.sink.PostgresSink;
import com.fraudguard.timing.TransactionTimeAssigner;
import com.fraudguard.util.TransactionDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import javax.xml.crypto.Data;
import java.time.Duration;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class FraudDetectorJob {

	public final void run() throws Exception{
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Transaction> source = createTransactionSource(env);
		KeyedStream<Transaction, String> keyedTransactionSource = source.keyBy(new TransactionKeySelector());

		DataStream<Alert> highFrequencyAlerts = keyedTransactionSource
				.window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
				.process(new HighFrequencyDetector());

		DataStream<Alert> impossibleLocationAlerts = keyedTransactionSource
				.process(new LocationDetectionFunction());

		DataStream<Alert> cepAlerts =  CEP.pattern(keyedTransactionSource, createFraudSpendPattern())
				.select(new SequenceAlertFunction());

		DataStream<Alert> alerts = highFrequencyAlerts.union(impossibleLocationAlerts)
				.union(cepAlerts);

		alerts.addSink(PostgresSink.createSink()).name("Postgres Sink");


		// Execute program, beginning computation.
		env.execute("Transaction Fraud Detection");

	}

	private Pattern<Transaction, ?> createFraudSpendPattern() {
		return Pattern.<Transaction>begin("firstLow")
				.where(new FraudSequenceFunction(50, true))

				.next("secondLow")
				.where(new FraudSequenceFunction(50, true))

				.next("highAmount")
				.where(new FraudSequenceFunction(1000, false))

				.within(Duration.ofSeconds(60));
	}

	private DataStream<Transaction> createTransactionSource(StreamExecutionEnvironment env) {
		var transactionSource = KafkaSource.<Transaction>builder()
				.setBootstrapServers("kafka:29092")
				.setTopics("transactions")
				.setGroupId("fraud-detector")
				.setStartingOffsets(OffsetsInitializer.latest())
				.setValueOnlyDeserializer(new TransactionDeserializer())
				.build();

		WatermarkStrategy<Transaction> watermarkStrategy = WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
				.withTimestampAssigner(new TransactionTimeAssigner())
				.withIdleness(Duration.ofSeconds(10));

		return env.fromSource(transactionSource, watermarkStrategy, "transaction-source", TypeInformation.of(Transaction.class));
	}

	public static void main(String[] args) throws Exception {
		FraudDetectorJob app = new FraudDetectorJob();
		app.run();
	}
}
