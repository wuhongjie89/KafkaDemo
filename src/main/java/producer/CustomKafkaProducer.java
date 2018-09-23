package producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.Future;

@Component
@Data
public abstract class CustomKafkaProducer {
    private final Class serializer = StringSerializer.class;

    private Producer<String, String> producer;

    private ObjectMapper mapper = new ObjectMapper();

    private void init() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServiceConfig());
        properties.put(ProducerConfig.ACKS_CONFIG, getAcksConfig());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
        this.producer = new KafkaProducer<>(properties);
    }

    abstract String getBootstrapServiceConfig();

    abstract String getAcksConfig();

    abstract int getMaxInFlightRequestPerConnection();

    /**
     * 获取topic名称，由子类实现
     *
     * @return topic 名称
     */
    abstract String getTopic();

    /**
     * 发送消息
     * 内部将传入的o对象转换为json字符串发送
     *
     * @param o 对象
     */
    public void send(Object o) {
        if (this.producer == null) {
            init();
        }
        try {
            String msg = mapper.writeValueAsString(o);
            Future<RecordMetadata> future = this.producer.send(new ProducerRecord<>(getTopic(), msg));
            future.get();
            System.out.println("send success.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
