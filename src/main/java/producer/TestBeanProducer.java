package producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TestBeanProducer extends CustomKafkaProducer {
    @Value("${test.bean.topic}")
    private String topic;

    @Value("${bootstrap.server.config}")
    private String bootstrapServerConfig;

    @Value("${acks.config}")
    private String acksConfig;
    @Value("${max.in.flight.requests.per.connection}")
    private int maxInFlightRequestPerConnection;

    @Override
    String getBootstrapServiceConfig() {
        return this.bootstrapServerConfig;
    }

    @Override
    String getAcksConfig() {
        return this.acksConfig;
    }

    @Override
    int getMaxInFlightRequestPerConnection() {
        return this.maxInFlightRequestPerConnection;
    }

    /**
     * 获取topic名称，由子类实现
     *
     * @return topic 名称
     */
    @Override
    String getTopic() {
        return this.topic;
    }
}
