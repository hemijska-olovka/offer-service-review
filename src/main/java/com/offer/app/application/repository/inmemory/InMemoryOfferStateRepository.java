package com.offer.app.application.repository.inmemory;

import com.offer.app.application.notification.StateChangeNotifier;
import com.offer.app.application.repository.EventRepository;
import com.offer.app.application.repository.MarketRepository;
import com.offer.app.application.repository.OfferStateRepository;
import com.offer.app.domain.model.Event;
import com.offer.app.domain.model.Market;
import com.offer.app.domain.model.OfferStateSnapshot;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

@Component
public final class InMemoryOfferStateRepository implements MarketRepository, EventRepository, OfferStateRepository {
    private final ConcurrentMap<String, Event> events =
            new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Market> markets =
            new ConcurrentHashMap<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final StateChangeNotifier stateChangeNotifier;

    public InMemoryOfferStateRepository(StateChangeNotifier stateChangeNotifier) {
        this.stateChangeNotifier = Objects.requireNonNull(stateChangeNotifier);;
    }

    @Override
    public Event updateAtomically(
            String eventId,
            Function<Optional<Event>, Event> updater
    ) {
        Event updated;
        boolean changed;

        lock.writeLock().lock();
        try {
            Event previous = events.get(eventId);

            updated = Objects.requireNonNull(
                    updater.apply(Optional.ofNullable(previous)),
                    "Updated Event cannot be null"
            );

            if (!eventId.equals(updated.id())) {
                throw new IllegalArgumentException(
                        "Updated Event ID does not match requested ID"
                );
            }

            changed = !updated.equals(previous);

            if (changed) {
                events.put(eventId, updated);
            }
        } finally {
            lock.writeLock().unlock();
        }

        if (changed) {
            stateChangeNotifier.eventChanged(eventId);
        }

        return updated;
    }

    @Override
    public void upsert(Market market) {
        Objects.requireNonNull(
                market,
                "Market cannot be null"
        );
        boolean changed;

        lock.writeLock().lock();

        try {
            Market previous = markets.get(market.id());

            changed = !market.equals(previous);

            if (changed) {
                markets.put(market.id(), market);
            }
        } finally {
            lock.writeLock().unlock();
        }
        if (changed) {
            stateChangeNotifier.marketChanged(market.id());
        }
    }

    @Override
    public Optional<Event> findEventById(String eventId) {
        Objects.requireNonNull(eventId);

        lock.readLock().lock();
        try {
            return Optional.ofNullable(events.get(eventId));
        } finally {
            lock.readLock().unlock();
        }
    }
    @Override
    public Optional<Market> findMarketById(String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException(
                    "Market ID cannot be null or blank"
            );
        }

        lock.readLock().lock();

        try {
            return Optional.ofNullable(markets.get(id));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void replaceSnapshot(OfferStateSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "Snapshot is required");

        Map<String, Event> newEvents = new HashMap<>();
        Map<String, Market> newMarkets = new HashMap<>();

        for (Event event : snapshot.events()) {
            newEvents.put(event.id(), event);
        }

        for (Market market : snapshot.markets()) {
            newMarkets.put(market.id(), market);
        }

        lock.writeLock().lock();

        try {
            events.clear();
            events.putAll(newEvents);

            markets.clear();
            markets.putAll(newMarkets);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public OfferStateSnapshot loadSnapshot() {
        lock.readLock().lock();

        try {
            List<Event> sortedEvents = events.values()
                    .stream()
                    .sorted(
                            Comparator.comparing(Event::startsAt)
                                    .thenComparing(Event::id)
                    )
                    .toList();

            List<Market> sortedMarkets = markets.values()
                    .stream()
                    .sorted(Comparator.comparing(Market::id))
                    .toList();

            return new OfferStateSnapshot(
                    sortedEvents,
                    sortedMarkets
            );
        } finally {
            lock.readLock().unlock();
        }
    }
}
