package com.offer.app.infrastructure.kafka.consumer;

import com.offer.app.application.helper.EventUpdateHelper;
import com.offer.app.application.usecase.CreateOrUpdateEventUseCase;
import com.offer.app.infrastructure.kafka.checkpoint.KafkaRecoverySession;
import com.offer.app.infrastructure.kafka.mapper.EventKafkaMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

@Component
public final class EventKafkaConsumer implements ConsumerSeekAware {

    private static final Logger log =
            LoggerFactory.getLogger(EventKafkaConsumer.class);

    private final CreateOrUpdateEventUseCase useCase;
    private final EventKafkaMapper mapper;
    private final ObjectProvider<KafkaRecoverySession> recoverySession;
    public EventKafkaConsumer(
            CreateOrUpdateEventUseCase useCase,
            EventKafkaMapper mapper, ObjectProvider<KafkaRecoverySession> kafkaRecoverySessionObjectProvider
    ) {
        this.useCase = Objects.requireNonNull(useCase);
        this.mapper = Objects.requireNonNull(mapper);
        this.recoverySession = kafkaRecoverySessionObjectProvider;
    }

    @KafkaListener(
            id = "eventKafkaConsumer",
            idIsGroup = false,
            topics = "${offer.kafka.topics.event}",
            concurrency = "2",
            ackMode = "MANUAL_IMMEDIATE"
    )
    public void consume(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment
    ) {
        try {
            EventUpdateHelper update =
                    mapper.parse(record.value());

            log.info(
                    "Event received. id={}, key={}, partition={}, offset={}",
                    update.id(),
                    record.key(),
                    record.partition(),
                    record.offset()
            );

            if (record.key() == null
                    || !record.key().equals(update.id())) {
                throw new IllegalArgumentException(
                        "Kafka record key must match Event.id"
                );
            }

            Runnable mutation = () -> {
                useCase.execute(update);
            };

            KafkaRecoverySession session =
                    recoverySession.getIfAvailable();

            if (session != null) {
                session.applyRecord(record, mutation);
            } else {
                mutation.run();
            }

            acknowledgment.acknowledge();

            log.info(
                    "Event processed successfully. id={}, partition={}, offset={}",
                    update.id(),
                    record.partition(),
                    record.offset()
            );

        } catch (RuntimeException ex) {
            log.error(
                    "Event processing FAILED. topic={}, partition={}, offset={}, key={}, payload={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    record.value(),
                    ex
            );

            throw ex;
        }
    }
    @Override
    public void onPartitionsAssigned(
            Map<TopicPartition, Long> assignments,
            ConsumerSeekCallback callback
    ) {
        recoverySession.ifAvailable(
                session -> session.seekAssigned(
                        assignments,
                        callback
                )
        );
    }
}
