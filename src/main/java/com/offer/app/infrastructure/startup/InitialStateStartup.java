package com.offer.app.infrastructure.startup;


import com.offer.app.application.usecase.InitializeOfferStateUseCase;

import com.offer.app.infrastructure.kafka.checkpoint.KafkaCheckpointStateRestorer;
import com.offer.app.infrastructure.kafka.checkpoint.KafkaRecoverySession;
import com.offer.app.infrastructure.notification.StateChangeDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;


@Component
public class InitialStateStartup implements ApplicationRunner {
    private static final Logger log =
            LoggerFactory.getLogger(InitialStateStartup.class);

    private final InitializeOfferStateUseCase loadInitialStateUseCase;
    private final KafkaListenerEndpointRegistry kafkaRegistry;
    private final ObjectProvider<KafkaCheckpointStateRestorer> stateRestorer;
    private final ObjectProvider<KafkaRecoverySession> recoverySession;
    private final StateChangeDispatcher stateChangeDispatcher;
    public InitialStateStartup(
            InitializeOfferStateUseCase loadInitialStateUseCase,
            KafkaListenerEndpointRegistry kafkaRegistry, ObjectProvider<KafkaCheckpointStateRestorer> stateRestorer, ObjectProvider<KafkaRecoverySession> recoverySession, StateChangeDispatcher stateChangeDispatcher
    ) {
        this.loadInitialStateUseCase = loadInitialStateUseCase;
        this.kafkaRegistry = kafkaRegistry;
        this.stateRestorer = stateRestorer;
        this.recoverySession = recoverySession;
        this.stateChangeDispatcher = stateChangeDispatcher;
    }

    @Override
    public void run(ApplicationArguments args) {
        // Load and initialize all Markets and Events
        loadInitialStateUseCase.execute();
        stateRestorer.ifAvailable(
                KafkaCheckpointStateRestorer::restore
        );
        recoverySession.ifAvailable(
                KafkaRecoverySession::prepare
        );
        stateChangeDispatcher.markRecoveryReady();

        // Kafka may start only after successful initialization
        kafkaRegistry.start();

    }
}
