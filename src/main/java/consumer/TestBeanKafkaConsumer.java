package consumer;

import entity.TestBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

@Component
public class TestBeanKafkaConsumer extends CustomKafkaConsumer<TestBean> {
    @Value("${bootstrap.server.config}")
    private String bootstrapServerConfig;

    @Value("${group.id}")
    private String groupID;

    @Value("${test.bean.topic}")
    private String topic;

    @Value("${max.poll.records.config}")
    private int maxPollRecordsConfig;

    @Override
    String getBootstrapServiceConfig() {
        return this.bootstrapServerConfig;
    }

    @Override
    int getMaxPollRecordsConfig() {
        return this.maxPollRecordsConfig;
    }

    @Override
    Class getTClass() {
        return TestBean.class;
    }

    @Override
    String getGroupID() {
        return this.groupID;
    }

    @Override
    Collection<String> getTopics() {
        return Collections.singletonList(this.topic);
    }

    @Override
    Duration getPollInterval() {
        return Duration.ofSeconds(1);
    }

    @Override
    Consumer<TestBean> getConsumer() {
        return this::consume;
    }

    private void consume(TestBean testBean) {
        System.out.println("receive success.");
        System.out.println(testBean);
    }
}
