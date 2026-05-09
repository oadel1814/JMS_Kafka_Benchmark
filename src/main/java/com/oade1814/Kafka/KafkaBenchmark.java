package com.oade1814.Kafka;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;

public class KafkaBenchmark {
    private static final String BOOTSTRAP_SERVERS    = "localhost:9092";
    private static final String TOPIC                = "test-topic";
    private static final int    NUM_MESSAGES         = 1000;
    private static final int    NUM_LATENCY_MESSAGES = 10_000;
    private static final String PAYLOAD_PATH         = "src/main/resources/message.txt";
    private static final int    MAX_THROUGHPUT_CAP   = 1600000;

    public static void main(String[] args) throws Exception {
        String payload = new String(Files.readAllBytes(Paths.get(PAYLOAD_PATH)));

        System.out.println("========================================");
        System.out.println("        KAFKA BENCHMARK SUITE           ");
        System.out.println("========================================\n");

//        measureProduceResponseTime(payload);
//        measureConsumeResponseTime();
        measureMaxProduceThroughput(payload);
//        measureMaxConsumeThroughput(payload);
//        measureMedianLatency(payload);

        System.out.println("\n========================================");
        System.out.println("         BENCHMARK COMPLETE             ");
        System.out.println("========================================");
    }

    private static KafkaProducer<String, String> buildProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return new KafkaProducer<>(props);
    }

    private static KafkaConsumer<String, String> buildConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "10");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return new KafkaConsumer<>(props);
    }


    private static void measureProduceResponseTime(String payload) throws Exception {
        System.out.println(">>> [1/5] Measuring Produce Response Time...");

        KafkaProducer<String, String> producer = buildProducer();
        List<Long> responseTimes = new ArrayList<>();

        for (int i = 0; i < NUM_MESSAGES; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, "key-" + i, payload);

            long start = System.nanoTime();
            producer.send(record).get(); // .get() makes it synchronous
            responseTimes.add(System.nanoTime() - start);
        }

        Collections.sort(responseTimes);
        long median = responseTimes.get(responseTimes.size() / 2);
        System.out.println("    Median Produce Response Time : " + median + " ns  (" + median / 1_000_000.0 + " ms)");
        System.out.println("    (Topic now has " + NUM_MESSAGES + " messages ready for consume test)\n");

        producer.close();
    }

    private static void measureConsumeResponseTime() throws Exception {
        System.out.println(">>> [2/5] Measuring Consume Response Time...");

        // Unique group ID so it always reads from the beginning
        KafkaConsumer<String, String> consumer =
                buildConsumer("response-time-group-" + System.currentTimeMillis());
        consumer.subscribe(Collections.singletonList(TOPIC));

        List<Long> responseTimes = new ArrayList<>();
        int count = 0;

        while (count < NUM_MESSAGES) {
            long start = System.nanoTime();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            long pollTime = System.nanoTime() - start;

            if (records.isEmpty()) {
                System.out.println("    WARNING: No records received — is the topic populated?");
                break;
            }

            for (ConsumerRecord<String, String> record : records) {
                // Divide poll time evenly across all records returned in this batch
                responseTimes.add(pollTime / records.count());
                count++;
                if (count >= NUM_MESSAGES) break;
            }
        }

        if (!responseTimes.isEmpty()) {
            Collections.sort(responseTimes);
            long median = responseTimes.get(responseTimes.size() / 2);
            System.out.println("    Median Consume Response Time : " + median + " ns  (" + median / 1_000_000.0 + " ms)\n");
        }

        consumer.close();
    }

    private static void measureMaxProduceThroughput(String payload) throws Exception {
        System.out.println(">>> [3/5] Measuring Max Produce Throughput...");

        int throughput = 100;
        int maxThroughput = 0;

        while (true) {
            KafkaProducer<String, String> producer = buildProducer();
            long periodNs = 1_000_000_000L / throughput;
            long sleepNs  = (long)(periodNs * 0.8);
            int failed = 0;

            for (int i = 0; i < throughput; i++) {
                try {
                    producer.send(new ProducerRecord<>(TOPIC, "key-" + i, payload)).get();
                } catch (Exception e) {
                    failed++;
                }
                Thread.sleep(sleepNs / 1_000_000, (int)(sleepNs % 1_000_000));
            }

            producer.close();
            System.out.println("    Tested: " + throughput + " msg/s | Failed: " + failed);

            if (failed > 0) {
                System.out.println("    Max Produce Throughput: " + maxThroughput + " msg/s\n");
                break;
            }

            maxThroughput = throughput;
            throughput *= 2;

            if (throughput > MAX_THROUGHPUT_CAP) {
                System.out.println("    Reached cap. Max Produce Throughput: >" + maxThroughput + " msg/s\n");
                break;
            }
        }
    }

    private static void measureMaxConsumeThroughput(String payload) throws Exception {
        System.out.println(">>> [4/5] Measuring Max Consume Throughput...");

        int throughput = 100;
        int maxThroughput = 0;

        while (true) {
            // Pre-fill topic with enough messages for this round
            KafkaProducer<String, String> producer = buildProducer();
            for (int i = 0; i < throughput; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "key-" + i, payload)).get();
            }
            producer.close();

            // Consume at target rate with unique group so it reads fresh each round
            KafkaConsumer<String, String> consumer =
                    buildConsumer("throughput-group-" + System.currentTimeMillis());
            consumer.subscribe(Collections.singletonList(TOPIC));

            long periodNs = 1_000_000_000L / throughput;
            long sleepNs  = (long)(periodNs * 0.8);
            int received  = 0;
            long deadline = System.currentTimeMillis() + 5000; // 5 second window

            while (received < throughput && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    received++;
                    Thread.sleep(sleepNs / 1_000_000, (int)(sleepNs % 1_000_000));
                    if (received >= throughput) break;
                }
            }

            int failed = throughput - received;
            consumer.close();

            System.out.println("    Tested: " + throughput + " msg/s | Failed: " + failed);

            if (failed > 0) {
                System.out.println("    Max Consume Throughput: " + maxThroughput + " msg/s\n");
                break;
            }

            maxThroughput = throughput;
            throughput *= 2;

            if (throughput > MAX_THROUGHPUT_CAP) {
                System.out.println("    Reached cap. Max Consume Throughput: >" + maxThroughput + " msg/s\n");
                break;
            }
        }
    }

    private static void purgeAndResetTopic() throws Exception {
        System.out.println("    Purging old messages from topic...");

        // Delete and recreate the topic
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (org.apache.kafka.clients.admin.AdminClient admin =
                     org.apache.kafka.clients.admin.AdminClient.create(adminProps)) {

            // Delete topic
            admin.deleteTopics(Collections.singletonList(TOPIC)).all().get();
            Thread.sleep(2000); // wait for deletion to propagate

            // Recreate topic
            admin.createTopics(Collections.singletonList(
                    new org.apache.kafka.clients.admin.NewTopic(TOPIC, 1, (short) 1)
            )).all().get();
            Thread.sleep(1000); // wait for creation
        }

        System.out.println("    Topic purged and recreated.\n");
    }

    private static void measureMedianLatency(String payload) throws Exception {
        System.out.println(">>> [5/5] Measuring Median Latency (" + NUM_LATENCY_MESSAGES + " messages)...");

        purgeAndResetTopic();

        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        String latencyGroupId = "latency-group-" + System.currentTimeMillis();

        // Latch signals producer that consumer is ready
        CountDownLatch consumerReady = new CountDownLatch(1);

        Thread consumerThread = new Thread(() -> {
            KafkaConsumer<String, String> consumer = buildConsumer(latencyGroupId);
            consumer.subscribe(Collections.singletonList(TOPIC));

            // Poll until rebalance is complete (assignment is non-empty)
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ofMillis(100));
            }

            // Signal producer that consumer is ready
            consumerReady.countDown();
            System.out.println("    Consumer ready, signaling producer...");

            int count = 0;
            while (count < NUM_LATENCY_MESSAGES) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(50));

                if (records.isEmpty()) {
                    System.out.println("    WARNING: Timed out at message " + count);
                    break;
                }

                for (ConsumerRecord<String, String> record : records) {
                    long receiveTime = System.currentTimeMillis();
                    try {
                        String[] parts = record.value().split("###", 2);
                        if (parts.length < 2) continue;

                        long sentTime = Long.parseLong(parts[0].trim());
                        long latency = receiveTime - sentTime;

                        if (latency < 0 || latency > 60_000) {
                            System.out.println("    Skipping suspicious value: " + latency + " ms");
                            continue;
                        }

                        latencies.add(latency);
                    } catch (Exception e) {
                        System.out.println("    Parse error: " + e.getMessage());
                    }
                    count++;
                    if (count >= NUM_LATENCY_MESSAGES) break;
                }
            }
            consumer.close();
        });

        Thread producerThread = new Thread(() -> {
            try {
                consumerReady.await();
                System.out.println("    Producer starting...");

                KafkaProducer<String, String> producer = buildProducer();
                for (int i = 0; i < NUM_LATENCY_MESSAGES; i++) {
                    String value = System.currentTimeMillis() + "###" + payload;
                    // Use send().get() to send one at a time and wait for ack
                    producer.send(new ProducerRecord<>(TOPIC, "latency-key-" + i, value)).get();
                }
                producer.flush();
                producer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        consumerThread.start();
        producerThread.start(); // start both immediately — producer waits on latch

        producerThread.join();
        consumerThread.join();

        if (!latencies.isEmpty()) {
            Collections.sort(latencies);
            long median = latencies.get(latencies.size() / 2);
            System.out.println("    Messages measured : " + latencies.size());
            System.out.println("    Median Latency    : " + median + " ms");
        } else {
            System.out.println("    No valid latency data collected.");
        }
    }
}