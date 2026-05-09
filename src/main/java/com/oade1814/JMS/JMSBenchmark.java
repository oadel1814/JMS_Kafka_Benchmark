package com.oade1814.JMS;

import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JMSBenchmark {
    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String QUEUE_NAME = "test_queue";
    private static final int NUM_MESSAGES = 1000;
    private static final int NUM_LATENCY_MESSAGES = 10_000;
    private static final String PAYLOAD_PATH = "src/main/resources/message.txt";

    public static void main(String[] args) throws Exception {
        String payload = new String(Files.readAllBytes(Paths.get(PAYLOAD_PATH)));

        System.out.println("========================================");
        System.out.println("         JMS BENCHMARK SUITE            ");
        System.out.println("========================================\n");

//        measureProduceResponseTime(payload);
//        measureConsumeResponseTime();
//        measureMaxProduceThroughput(payload);
        measureMaxConsumeThroughput(payload);
//        measureMedianLatency(payload);

        System.out.println("\n========================================");
        System.out.println("         BENCHMARK COMPLETE             ");
        System.out.println("========================================");
    }


    private static void measureProduceResponseTime(String payload) throws Exception {
        System.out.println(">>> [1/5] Measuring Produce Response Time...");

        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(QUEUE_NAME);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        List<Long> responseTimes = new ArrayList<>();

        for (int i = 0; i < NUM_MESSAGES; i++) {
            TextMessage message = session.createTextMessage(payload);
            long start = System.nanoTime();
            producer.send(message); // API call to broker (blocking call)
            responseTimes.add(System.nanoTime() - start);
        }

        Collections.sort(responseTimes);
        long median = responseTimes.get(responseTimes.size() / 2);
        System.out.println("    Median Produce Response Time : " + median + " ns  (" + median / 1_000_000.0 + " ms)");
        System.out.println("    (Queue now has " + NUM_MESSAGES + " messages ready for consume test)\n");

        producer.close();
        session.close();
        connection.close();
    }

    private static void measureConsumeResponseTime() throws Exception {
        System.out.println(">>> [2/5] Measuring Consume Response Time...");

        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(QUEUE_NAME);
        MessageConsumer consumer = session.createConsumer(destination);

        List<Long> responseTimes = new ArrayList<>();

        for (int i = 0; i < NUM_MESSAGES; i++) {
            long start = System.nanoTime();
            Message message = consumer.receive(5000);
            if (message == null) {
                System.out.println("    WARNING: Queue ran out of messages at message " + (i + 1));
                break;
            }

            responseTimes.add(System.nanoTime() - start);
        }

        if (!responseTimes.isEmpty()) {
            Collections.sort(responseTimes);
            long median = responseTimes.get(responseTimes.size() / 2);
            System.out.println("    Median Consume Response Time : " + median + " ns  (" + median / 1_000_000.0 + " ms)\n");
        }

        consumer.close();
        session.close();
        connection.close();
    }

    private static void measureMaxProduceThroughput(String payload) throws Exception {
        System.out.println(">>> [3/5] Measuring Max Produce Throughput...");

        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(QUEUE_NAME);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        int throughput = 100;
        int maxThroughput = 0;

        while (true) {
            long periodNs = 1_000_000_000L / throughput;
            long sleepNs  = (long)(periodNs * 0.8);
            int failed = 0;

            // ── Key addition: measure actual elapsed time ──
            long testStart = System.nanoTime();

            for (int i = 0; i < throughput; i++) {
                try {
                    producer.send(session.createTextMessage(payload));
                } catch (Exception e) {
                    failed++;
                }
                if (sleepNs > 500_000) { // only sleep if > 0.5ms, otherwise useless
                    Thread.sleep(sleepNs / 1_000_000, (int)(sleepNs % 1_000_000));
                }
            }

            long actualElapsedNs = System.nanoTime() - testStart;
            double actualElapsedSec = actualElapsedNs / 1_000_000_000.0;

            // Real throughput = messages sent / actual time taken
            double realThroughput = throughput / actualElapsedSec;

            // Valid = all sent within 1.2 seconds (20% tolerance)
            boolean withinTimeWindow = actualElapsedSec <= 1.2;

            System.out.printf("    Tested: %,d msg/s | Failed: %d | " +
                            "Elapsed: %.3f s | Real rate: %.0f msg/s | %s%n",
                    throughput, failed, actualElapsedSec, realThroughput,
                    withinTimeWindow ? "VALID ✓" : "INVALID — took too long ✗");

            if (failed > 0 || !withinTimeWindow) {
                System.out.println("    Max Produce Throughput: " + maxThroughput + " msg/s\n");
                break;
            }

            maxThroughput = throughput;
            throughput *= 2;

            if (throughput > 100_000) {
                System.out.println("    Reached cap: >" + maxThroughput + " msg/s\n");
                break;
            }
        }

        producer.close();
        session.close();
        connection.close();
    }

    private static void measureMaxConsumeThroughput(String payload) throws Exception {
        System.out.println(">>> [4/5] Measuring Max Consume Throughput...");

        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(QUEUE_NAME);

        int throughput = 100;
        int maxThroughput = 0;

        while (true) {
            // Pre-fill queue for this round
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            for (int i = 0; i < throughput; i++) {
                producer.send(session.createTextMessage(payload));
            }
            producer.close();

            // Consume at target rate
            MessageConsumer consumer = session.createConsumer(destination);
            long periodNs = 1_000_000_000L / throughput;
            long sleepNs  = (long)(periodNs * 0.8);
            int failed = 0;

            // ── Elapsed time validation (same as producer) ──
            long testStart = System.nanoTime();

            for (int i = 0; i < throughput; i++) {
                long start = System.nanoTime();
                Message message = consumer.receive(1000);
                long responseTime = System.nanoTime() - start;

                if (message == null) failed++;

                if (sleepNs > 500_000) { // only sleep if > 0.5ms, otherwise useless
                    Thread.sleep(sleepNs / 1_000_000, (int)(sleepNs % 1_000_000));
                }
            }

            long actualElapsedNs = System.nanoTime() - testStart;
            double actualElapsedSec = actualElapsedNs / 1_000_000_000.0;
            double realThroughput = throughput / actualElapsedSec;
            boolean withinTimeWindow = actualElapsedSec <= 1.2;

            System.out.printf("    Tested: %,d msg/s | Failed: %d | " +
                            "Elapsed: %.3f s | Real rate: %.0f msg/s | %s%n",
                    throughput, failed, actualElapsedSec, realThroughput,
                    withinTimeWindow ? "VALID ✓" : "INVALID — took too long ✗");

            if (failed > 0 || !withinTimeWindow) {
                System.out.println("    Max Consume Throughput: " + maxThroughput + " msg/s\n");
                consumer.close();
                break;
            }

            maxThroughput = throughput;
            throughput *= 2;
            consumer.close();

            if (throughput > 100_000) {
                System.out.println("    Reached cap. Max Consume Throughput: >" + maxThroughput + " msg/s\n");
                break;
            }
        }

        session.close();
        connection.close();
    }

    private static void measureMedianLatency(String payload) throws Exception {
        System.out.println(">>> [5/5] Measuring Median Latency (" + NUM_LATENCY_MESSAGES + " messages)...");

        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());

        // Latch — producer waits until consumer is ready
        java.util.concurrent.CountDownLatch consumerReady =
                new java.util.concurrent.CountDownLatch(1);

        // Consumer thread
        Thread consumerThread = new Thread(() -> {
            try {
                Connection conn = factory.createConnection();
                conn.start();
                Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));

                // Signal producer that consumer is ready
                consumerReady.countDown();
                System.out.println("    Consumer ready, signaling producer...");

                for (int i = 0; i < NUM_LATENCY_MESSAGES; i++) {
                    Message message = consumer.receive(10000);
                    long receiveTime = System.currentTimeMillis();

                    if (message == null) {
                        System.out.println("    WARNING: Timed out at message " + i);
                        break;
                    }

                    if (!message.propertyExists("sentTimestamp")) continue;

                    long sentTime = message.getLongProperty("sentTimestamp");
                    long latency = receiveTime - sentTime;

                    // Sanity check
                    if (latency < 0 || latency > 60_000) {
                        System.out.println("    Skipping suspicious value: " + latency + " ms");
                        continue;
                    }

                    latencies.add(latency);
                }

                consumer.close();
                session.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Producer thread
        Thread producerThread = new Thread(() -> {
            try {
                // Wait until consumer is fully ready
                consumerReady.await();
                System.out.println("    Producer starting...");

                Connection conn = factory.createConnection();
                conn.start();
                Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                for (int i = 0; i < NUM_LATENCY_MESSAGES; i++) {
                    TextMessage message = session.createTextMessage(payload);
                    message.setLongProperty("sentTimestamp", System.currentTimeMillis());
                    // send() in JMS is already synchronous — waits for broker ack
                    producer.send(message);
                }

                producer.close();
                session.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Start both — producer waits on latch internally
        consumerThread.start();
        producerThread.start();

        producerThread.join();
        consumerThread.join();

        if (!latencies.isEmpty()) {
            Collections.sort(latencies);
            long median = latencies.get(latencies.size() / 2);
            System.out.println("    Messages measured : " + latencies.size());
            System.out.println("    Median Latency    : " + median + " ms");
        } else {
            System.out.println("    No latency data collected.");
        }
    }

}