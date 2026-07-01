package edu.eci.arsw.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name("orders")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic paymentsTopic() {
        return TopicBuilder.name("payments")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic inventoryTopic() {
        return TopicBuilder.name("inventory")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic notificationsTopic() {
        return TopicBuilder.name("notifications")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic ordersDltTopic() {
        return TopicBuilder.name("orders.DLT")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic paymentsDltTopic() {
        return TopicBuilder.name("payments.DLT")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic inventoryDltTopic() {
        return TopicBuilder.name("inventory.DLT")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
