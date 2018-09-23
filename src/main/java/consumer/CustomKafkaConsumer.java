package consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.function.Consumer;

@Data
@Component
public abstract class CustomKafkaConsumer<T> implements Runnable {
    private final Class deserializer = StringDeserializer.class;

    private KafkaConsumer<String, String> consumer;

    private ObjectMapper mapper;

    private Class<T> tClass;

    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void run() {
        init();
        Consumer<T> tConsumer = getConsumer();
        Duration duration = getPollInterval();
        Collection<String> topics = getTopics();
        consumer.subscribe(topics);
        while (true) {
            try {
                ConsumerRecords<String, String> pollRecords = consumer.poll(duration);
                consume(tConsumer, topics, pollRecords);
                consumer.commitSync();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void consume(Consumer<T> tConsumer, Collection<String> topics, ConsumerRecords<String, String> pollRecords) {
        topics.stream()
                .map(pollRecords::records)
                .map(Iterable::iterator)
                .forEach(o -> o.forEachRemaining(x -> tConsumer.accept(this.map(x.value()))));
    }

    @SuppressWarnings("unchecked")
    private void init() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServiceConfig());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, getGroupID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, getMaxPollRecordsConfig());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.deserializer);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.deserializer);
        this.consumer = new KafkaConsumer<>(properties);
        this.mapper = new ObjectMapper();
//        this.tClass = (Class<T>) ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        this.tClass = getTClass();
    }

    abstract String getBootstrapServiceConfig();

    abstract int getMaxPollRecordsConfig();

    abstract Class getTClass();

    abstract String getGroupID();

    abstract Collection<String> getTopics();

    abstract Duration getPollInterval();

    private T map(String msg) {
        try {
            return mapper.readValue(msg, this.tClass);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    abstract Consumer<T> getConsumer();
}
