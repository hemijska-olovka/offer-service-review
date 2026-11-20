package com.offer.app.infrastructure.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class EventTopicConfiguration {

    @Bean
    public NewTopic eventTopic(
            @Value("${offer.kafka.topics.event}") String topicName
    ) {
        return TopicBuilder.name(topicName)
                .partitions(2)
                .replicas(1)
                .build();
    }
}
