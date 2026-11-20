package com.offer.app.infrastructure.notification;

import com.offer.app.application.repository.EventRepository;
import com.offer.app.application.repository.MarketRepository;
import com.offer.app.domain.model.Event;
import com.offer.app.domain.model.Market;
import com.offer.app.infrastructure.kafka.checkpoint.KafkaCheckpointSnapshotService;
import com.offer.app.infrastructure.persistence.OfferStatePersistence;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Component
public final class StateChangeDispatcher {
    private final ObjectProvider<MarketRepository> marketRepository;
    private final ObjectProvider<EventRepository> eventRepository;
    private final ObjectProvider<OfferStatePersistence> persistence;
    private final ObjectProvider<KafkaCheckpointSnapshotService>
            checkpointSnapshotService;

    private final CountDownLatch recoveryReady =
            new CountDownLatch(1);
    public StateChangeDispatcher(
            ObjectProvider<MarketRepository> marketRepository,
            ObjectProvider<EventRepository> eventRepository,
            ObjectProvider<OfferStatePersistence> persistence, ObjectProvider<KafkaCheckpointSnapshotService> checkpointSnapshotService
    ) {
        this.marketRepository = marketRepository;
        this.eventRepository = eventRepository;
        this.persistence = persistence;
        this.checkpointSnapshotService = checkpointSnapshotService;
    }

    private static final Logger log =
            LoggerFactory.getLogger(StateChangeDispatcher.class);

    private static final Boolean WAKE_UP = Boolean.TRUE;

    private final BlockingQueue<Boolean> signals =
            new ArrayBlockingQueue<>(1);

    private final ConcurrentMap<String, Long> pendingMarkets =
            new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Long> pendingEvents =
            new ConcurrentHashMap<>();

    private final AtomicLong sequence = new AtomicLong();

    private volatile boolean running;
    private Thread worker;
    public void markRecoveryReady() {
        recoveryReady.countDown();
    }
    @PostConstruct
    public void start() {
        running = true;

        worker = Thread.ofPlatform()
                .name("offer-state-worker")
                .start(this::runLoop);

        log.info("StateChangeDispatcher started");
    }

    public void marketChanged(String marketId) {
        Objects.requireNonNull(marketId);

        pendingMarkets.merge(
                marketId,
                sequence.incrementAndGet(),
                Math::max
        );

        signals.offer(WAKE_UP);
    }

    public void eventChanged(String eventId) {
        Objects.requireNonNull(eventId);

        pendingEvents.merge(
                eventId,
                sequence.incrementAndGet(),
                Math::max
        );

        signals.offer(WAKE_UP);
    }
    private void runLoop() {
        while (running) {
            try {
                signals.take();

                while (running && hasPendingChanges()) {
                    boolean failed = processPendingChanges();

                    if (failed) {
                        // Retry without busy looping.
                        Thread.sleep(1000);
                    }
                }

            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;

            } catch (RuntimeException ex) {
                log.error(
                        "Unexpected persistence worker error",
                        ex
                );

                // Pending changes remain available.
                if (running) {
                    signals.offer(WAKE_UP);
                }
            }
        }

        log.info("StateChangeDispatcher stopped");
    }

    private boolean hasPendingChanges() {
        return !pendingMarkets.isEmpty()
                || !pendingEvents.isEmpty();
    }
    private boolean hasPendingMarketChanges() {
        return !pendingMarkets.isEmpty();
    }

    private boolean hasPendingEventChanges() {
        return !pendingEvents.isEmpty();
    }
    private boolean processPendingChanges() {

        boolean hasFailures = false;

        Map<String, Long> markets =
                Map.copyOf(pendingMarkets);

        Map<String, Long> events =
                Map.copyOf(pendingEvents);

        KafkaCheckpointSnapshotService checkpointService =
                checkpointSnapshotService.getIfAvailable();

        // PostgreSQL checkpoint persistence
        if (checkpointService != null) {

            try {
                recoveryReady.await();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while waiting for recovery", ex);
                return true;
            }

            if (!markets.isEmpty()) {
                try {
                    checkpointService.persistMarketCheckpoint();

                    markets.forEach((id, revision) ->
                            pendingMarkets.remove(id, revision)
                    );

                    log.info(
                            "Market checkpoint persisted successfully. count={}",
                            markets.size()
                    );

                } catch (RuntimeException ex) {
                    hasFailures = true;
                    log.error("Market checkpoint persistence failed", ex);
                }
            }

            if (!events.isEmpty()) {
                try {
                    checkpointService.persistEventCheckpoint();

                    events.forEach((id, revision) ->
                            pendingEvents.remove(id, revision)
                    );

                    log.info(
                            "Event checkpoint persisted successfully. count={}",
                            events.size()
                    );

                } catch (RuntimeException ex) {
                    hasFailures = true;
                    log.error("Event checkpoint persistence failed", ex);
                }
            }

            return hasFailures;
        }

        // Existing persistence for non-postgres profiles
        for (var entry : markets.entrySet()) {

            String id = entry.getKey();
            Long revision = entry.getValue();

            try {
                Market market = marketRepository.getObject()
                        .findMarketById(id)
                        .orElseThrow(() ->
                                new IllegalStateException(
                                        "Market not found: " + id
                                )
                        );

                persistence.getObject().upsertMarket(market);

                pendingMarkets.remove(id, revision);

                log.info(
                        "Market persisted successfully. id={}, revision={}",
                        id,
                        revision
                );

            } catch (RuntimeException ex) {
                hasFailures = true;

                log.error(
                        "Market persistence failed. id={}, revision={}",
                        id,
                        revision,
                        ex
                );
            }
        }

        for (var entry : events.entrySet()) {

            String id = entry.getKey();
            Long revision = entry.getValue();

            try {
                Event event = eventRepository.getObject()
                        .findEventById(id)
                        .orElseThrow(() ->
                                new IllegalStateException(
                                        "Event not found: " + id
                                )
                        );

                persistence.getObject().upsertEvent(event);

                pendingEvents.remove(id, revision);

                log.info(
                        "Event persisted successfully. id={}, revision={}",
                        id,
                        revision
                );

            } catch (RuntimeException ex) {
                hasFailures = true;

                log.error(
                        "Event persistence failed. id={}, revision={}",
                        id,
                        revision,
                        ex
                );
            }
        }

        return hasFailures;
    }

    @PreDestroy
    public void shutdown() {
        running = false;

        if (worker != null) {
            worker.interrupt();

            try {
                worker.join(2000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean persistCheckpointIfConfigured(
            boolean marketChanged,
            boolean eventChanged
    ) {
        KafkaCheckpointSnapshotService service =
                checkpointSnapshotService.getIfAvailable();

        if (service == null) {
            // Existing non-postgres persistence remains active.
            return false;
        }

        try {
            recoveryReady.await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();

            throw new IllegalStateException(
                    "Interrupted while waiting for Kafka recovery",
                    exception
            );
        }

        if (marketChanged) {
            service.persistMarketCheckpoint();
        }

        if (eventChanged) {
            service.persistEventCheckpoint();
        }

        return true;
    }
}
