package demo;

import consumer.CustomKafkaConsumer;
import consumer.TestBeanKafkaConsumer;
import entity.TestBean;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import producer.CustomKafkaProducer;
import producer.TestBeanProducer;

import java.util.concurrent.CompletableFuture;

public class DemoMain {
    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("application.xml");
        context.start();
        CustomKafkaConsumer consumer = context.getBean(TestBeanKafkaConsumer.class);
        CustomKafkaProducer producer = context.getBean(TestBeanProducer.class);

        //start consumer
        CompletableFuture.runAsync(consumer);

        //start producer every 5 second
        CompletableFuture.runAsync(() -> {
            while (true) {
                try {
                    producer.send(new TestBean());
                    Thread.sleep(5000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        //hold main thread
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
