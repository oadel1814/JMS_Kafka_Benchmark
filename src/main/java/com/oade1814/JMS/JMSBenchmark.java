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
    private static final int NUM_MESSAGES  = 1000;   // messages per round
    private static final int NUM_ROUNDS    = 1000;   // rounds per benchmark phase
    private static final int NUM_LATENCY_MESSAGES = 10_000;
    private static final String PAYLOAD_PATH = "src/main/resources/message.txt";

    // ms to wait before declaring queue empty during drain
    private static final int DRAIN_TIMEOUT_MS = 500;

    public static void main(String[] args) throws Exception {
        String payload = new String(Files.readAllBytes(Paths.get(PAYLOAD_PATH)));

        System.out.println("========================================");
        System.out.println("         JMS BENCHMARK SUITE            ");
        System.out.println("========================================\n");

        // Step 0 – empty the queue so produce starts from a clean slate
        preDrain();

        // Step 1 – NUM_ROUNDS x NUM_MESSAGES send() calls → median of 1 000 000 samples
        measureProduceResponseTime(payload);

        // Step 2 – delete all messages in the queue EXCEPT NUM_MESSAGES
        deleteAllExcept(NUM_MESSAGES, payload);

        // Step 3 – NUM_ROUNDS x NUM_MESSAGES receive() calls → median of 1 000 000 samples
        measureConsumeResponseTime(payload);

//        measureMaxProduceThroughput(payload);
//        measureMaxConsumeThroughput(payload);
//        measureMedianLatency(payload);

        System.out.println("\n========================================");
        System.out.println("         BENCHMARK COMPLETE             ");
        System.out.println("========================================");
    }

    // ── 0. Pre-drain ───────────────────────────────────────────────────────────

    private static void preDrain() throws Exception {
        System.out.println(">>> [0/3] Pre-draining queue...");
        int removed = 0;
        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        try (Connection connection = factory.createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));
            while (consumer.receive(DRAIN_TIMEOUT_MS) != null) {
                removed++;
            }
        }
        System.out.println("    Removed " + removed + " pre-existing messages. Queue is now empty.\n");
    }

    // ── 1. Produce ─────────────────────────────────────────────────────────────

    /**
     * NUM_ROUNDS rounds x NUM_MESSAGES send() calls per round.
     * Each individual send() is timed → total samples = NUM_ROUNDS * NUM_MESSAGES = 1 000 000.
     * Median reported across all samples.
     *
     * After this method the queue contains NUM_ROUNDS * NUM_MESSAGES messages.
     */
    private static void measureProduceResponseTime(String payload) throws Exception {
        System.out.println(">>> [1/3] Measuring Produce Response Time...");
        System.out.println("    Running " + NUM_ROUNDS + " rounds x " + NUM_MESSAGES
                + " send() calls = " + (long) NUM_ROUNDS * NUM_MESSAGES + " total samples...");

        List<Long> allTimes = new ArrayList<>(NUM_ROUNDS * NUM_MESSAGES);

        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        try (Connection connection = factory.createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(QUEUE_NAME);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            for (int round = 0; round < NUM_ROUNDS; round++) {
                for (int i = 0; i < NUM_MESSAGES; i++) {
                    TextMessage message = session.createTextMessage(payload);
                    long start = System.nanoTime();
                    producer.send(message);                      // blocking call
                    allTimes.add(System.nanoTime() - start);
                }

                if ((round + 1) % 100 == 0) {
                    System.out.println("    ... completed " + (round + 1)
                            + " / " + NUM_ROUNDS + " rounds");
                }
            }
        }

        printMedian("Produce", allTimes);
        System.out.println("    (Queue now contains " + (long) NUM_ROUNDS * NUM_MESSAGES + " messages)\n");
    }

    // ── 2. Delete all except NUM_MESSAGES ──────────────────────────────────────

    private static void deleteAllExcept(int keepCount, String payload) throws Exception {
        System.out.println(">>> [2/3] Deleting all messages except " + keepCount + "...");

        List<String> kept = new ArrayList<>(keepCount);

        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        try (Connection connection = factory.createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(QUEUE_NAME);

            MessageConsumer consumer = session.createConsumer(destination);
            Message msg;
            int total = 0;
            while ((msg = consumer.receive(DRAIN_TIMEOUT_MS)) != null) {
                total++;
                if (kept.size() < keepCount) {
                    kept.add(((TextMessage) msg).getText());
                }
                // messages beyond keepCount are acknowledged and discarded
            }
            consumer.close();

            int surplus = Math.max(0, total - keepCount);
            System.out.println("    Found " + total + " messages; discarded "
                    + surplus + " surplus, keeping " + kept.size() + ".");

            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            for (String text : kept) {
                producer.send(session.createTextMessage(text));
            }
            producer.close();
        }

        System.out.println("    Queue now holds exactly " + kept.size() + " messages.\n");
    }

    // ── 3. Consume ─────────────────────────────────────────────────────────────

    /**
     * NUM_ROUNDS rounds x NUM_MESSAGES receive() calls per round.
     * Each individual receive() is timed → total samples = NUM_ROUNDS * NUM_MESSAGES = 1 000 000.
     * Median reported across all samples.
     *
     * After each round the queue is refilled with the just-consumed messages
     * so every round starts with exactly NUM_MESSAGES messages waiting.
     */
    private static void measureConsumeResponseTime(String payload) throws Exception {
        System.out.println(">>> [3/3] Measuring Consume Response Time...");
        System.out.println("    Running " + NUM_ROUNDS + " rounds x " + NUM_MESSAGES
                + " receive() calls = " + (long) NUM_ROUNDS * NUM_MESSAGES + " total samples...");

        List<Long> allTimes = new ArrayList<>(NUM_ROUNDS * NUM_MESSAGES);

        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        try (Connection connection = factory.createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(QUEUE_NAME);
            MessageConsumer consumer = session.createConsumer(destination);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            for (int round = 0; round < NUM_ROUNDS; round++) {

                List<String> consumed = new ArrayList<>(NUM_MESSAGES);
                boolean exhausted = false;

                for (int i = 0; i < NUM_MESSAGES; i++) {
                    long start = System.nanoTime();
                    Message message = consumer.receive(5_000);  // 5 s timeout
                    long elapsed = System.nanoTime() - start;

                    if (message == null) {
                        System.out.println("    WARNING: Round " + (round + 1)
                                + " – queue exhausted at message " + (i + 1)
                                + ". Results based on " + allTimes.size() + " samples so far.");
                        exhausted = true;
                        break;
                    }
                    allTimes.add(elapsed);
                    consumed.add(((TextMessage) message).getText());
                }

                if (exhausted) break;

                // Refill queue for the next round (skip after the last round)
                if (round < NUM_ROUNDS - 1) {
                    for (String text : consumed) {
                        producer.send(session.createTextMessage(text));
                    }
                }

                if ((round + 1) % 100 == 0) {
                    System.out.println("    ... completed " + (round + 1)
                            + " / " + NUM_ROUNDS + " rounds");
                }
            }
        }

        if (!allTimes.isEmpty()) {
            printMedian("Consume", allTimes);
        }
    }

    // ── Utility ────────────────────────────────────────────────────────────────

    private static void printMedian(String label, List<Long> times) {
        Collections.sort(times);
        int n = times.size();
        long median = times.get(n / 2);          // upper-middle for even n
        System.out.printf("    %-8s  samples=%d%n", label, n);
        System.out.printf("             median = %,12d ns  (%8.3f ms)%n", median, median / 1e6);
    }

    // ── Commented-out benchmarks (unchanged) ───────────────────────────────────

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

            long testStart = System.nanoTime();

            for (int i = 0; i < throughput; i++) {
                try {
                    producer.send(session.createTextMessage(payload));
                } catch (Exception e) {
                    failed++;
                }
                if (sleepNs > 500_000) {
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
                    withinTimeWindow ? "VALID" : "INVALID – took too long");

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
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            for (int i = 0; i < throughput; i++) {
                producer.send(session.createTextMessage(payload));
            }
            producer.close();

            MessageConsumer consumer = session.createConsumer(destination);
            long periodNs = 1_000_000_000L / throughput;
            long sleepNs  = (long)(periodNs * 0.8);
            int failed = 0;

            long testStart = System.nanoTime();

            for (int i = 0; i < throughput; i++) {
                Message message = consumer.receive(1000);
                if (message == null) failed++;
                if (sleepNs > 500_000) {
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
                    withinTimeWindow ? "VALID" : "INVALID – took too long");

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

        java.util.concurrent.CountDownLatch consumerReady =
                new java.util.concurrent.CountDownLatch(1);

        Thread consumerThread = new Thread(() -> {
            try {
                Connection conn = factory.createConnection();
                conn.start();
                Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));

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

        Thread producerThread = new Thread(() -> {
            try {
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
                    producer.send(message);
                }

                producer.close();
                session.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

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