package net.karlsonn.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.setOut;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.concurrent.CompletableFuture.runAsync;

public class CQRS {

    private static Logger logger = LoggerFactory.getLogger(CQRS.class);


    private static KafkaProducer<String, String> createProducer() {
        return new KafkaProducer<>(producerOpts());
    }

    private static Map<String, Object> producerOpts() {
        Map<String, Object> producerOpts = new HashMap<>();
        producerOpts.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerOpts.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerOpts.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return producerOpts;
    }

    private static KafkaConsumer<String, String> createConsumer() {
        return new KafkaConsumer<>(consumerOpts());
    }

    private static Map<String, Object> consumerOpts() {
        Map<String, Object> consumerOpts = new HashMap<>();
        consumerOpts.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerOpts.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        consumerOpts.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerOpts.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerOpts.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return consumerOpts;
    }

    public static void main(String[] args) throws InterruptedException {
        String rpc1Topic = "test";
        int howMuchWaitRequest = 50;
        int howMuchWaitResult = 500;
        Integer parameter = 3;

        KafkaConsumer<String, String> requestConsumer = createConsumer();
        KafkaProducer<String, String> resultProducer = createProducer();
        KafkaProducer<String, String> requestProducer = createProducer();
        KafkaConsumer<String, String> resultConsumer = createConsumer();

        requestConsumer.subscribe(Collections.singletonList(rpc1Topic));

        runAsync(() -> requestConsumer.poll(Duration.ofSeconds(howMuchWaitRequest)).forEach(v -> {
            String replyTo = new String(v.headers().lastHeader("replyTo").value());
            try {
                Integer result = square(Integer.parseInt(v.value()));
                resultProducer.send(new ProducerRecord<>(replyTo, 0, currentTimeMillis(), "key", result.toString()));
            } catch (Throwable ex) {
                System.out.println(ex.toString());;
            }
        }));


        String replyToTopic = UUID.randomUUID().toString() + "_" + rpc1Topic;
        requestProducer.send(new ProducerRecord<>(rpc1Topic, 0, currentTimeMillis(), "key", parameter.toString(), asList(new RecordHeader("replyTo", replyToTopic.getBytes()))));

        resultConsumer.subscribe(singleton(replyToTopic));

        runAsync(() -> resultConsumer.poll(Duration.ofSeconds(howMuchWaitResult)).forEach(v ->
                logger.info("res:" + v)));

        Thread.sleep(30000);
    }

    private static Integer square(Integer parameter) {
        return parameter * parameter;
    }
}
