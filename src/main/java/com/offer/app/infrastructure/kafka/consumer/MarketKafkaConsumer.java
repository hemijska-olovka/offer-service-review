package com.offer.app.infrastructure.kafka.consumer;

import com.offer.app.application.usecase.CreateOrUpdateMarketUseCase;
import com.offer.app.domain.model.Market;
import com.offer.app.infrastructure.kafka.checkpoint.KafkaRecoverySession;
import com.offer.app.infrastructure.kafka.mapper.MarketKafkaMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

@Component
public final class MarketKafkaConsumer implements ConsumerSeekAware {
    private final ObjectProvider<KafkaRecoverySession> recoverySession;
    private static final Logger log =
            LoggerFactory.getLogger(MarketKafkaConsumer.class);

    private final CreateOrUpdateMarketUseCase createOrUpdateMarketUseCase;
    private final MarketKafkaMapper mapper;

    public MarketKafkaConsumer(
            ObjectProvider<KafkaRecoverySession> recoverySession,
            CreateOrUpdateMarketUseCase useCase,
            MarketKafkaMapper mapper
    ) {
        this.recoverySession = recoverySession;
        this.createOrUpdateMarketUseCase = Objects.requireNonNull(useCase);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @KafkaListener(
            id = "marketKafkaConsumer",
            idIsGroup = false,
            topics = "${offer.kafka.topics.market}",
            ackMode = "MANUAL_IMMEDIATE"
    )
    public void consume(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment
    ) {
        try {
            Market market = mapper.deserialize(record.value());

            Runnable mutation = () ->
                    createOrUpdateMarketUseCase.execute(market);

            KafkaRecoverySession session =
                    recoverySession.getIfAvailable();

            if (session != null) {
                session.applyRecord(record, mutation);
            } else {
                mutation.run();
            }

            acknowledgment.acknowledge();

            log.info(
                    "Market processed successfully. id={}, offset={}",
                    market.id(),
                    record.offset()
            );

        } catch (RuntimeException ex) {
            log.error(
                    "Market processing failed. topic={}, partition={}, offset={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
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
