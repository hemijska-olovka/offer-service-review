package com.offer.app.application.mapper;

import java.util.*;
import com.offer.app.application.dto.OfferResponse;
import com.offer.app.application.dto.OfferResponse.EventDto;
import com.offer.app.application.dto.OfferResponse.EventMarketDto;
import com.offer.app.application.dto.OfferResponse.EventMarketOutcomeDto;
import com.offer.app.application.dto.OfferResponse.MarketDto;
import com.offer.app.application.dto.OfferResponse.OutcomeDto;
import com.offer.app.domain.model.Event;
import com.offer.app.domain.model.EventMarket;
import com.offer.app.domain.model.EventMarketOutcome;
import com.offer.app.domain.model.Market;
import com.offer.app.domain.model.MarketOutcome;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Component
public final class OfferResponseAssembler {
    public OfferResponse assemble(
            List<Event> events,
            List<Market> markets,
            Set<String> requestedEventIds
    ) {
        Objects.requireNonNull(events, "events");
        Objects.requireNonNull(markets, "markets");

        Map<String, Market> marketById = new HashMap<>();
        for (Market market : markets) {
            Market previous = marketById.putIfAbsent(market.id(), market);
            if (previous != null && !previous.equals(market)) {
                throw new IllegalStateException("Conflicting Market definitions for ID: " + market.id());
            }
        }

        // LinkedHashMaps deduplicate definitions while providing predictable output order.
        Map<String, MarketDto> usedMarkets = new LinkedHashMap<>();
        Map<String, OutcomeDto> usedOutcomes = new LinkedHashMap<>();
        List<EventDto> responseEvents = new ArrayList<>();
        Set<String> seenEventIds = new HashSet<>();

        for (Event event : events.stream().sorted(Comparator.comparing(Event::id)).toList()) {
            if (requestedEventIds != null && !requestedEventIds.contains(event.id())) {
                continue;
            }
            if (!seenEventIds.add(event.id())) {
                throw new IllegalStateException("Duplicate Event ID: " + event.id());
            }
            // A betting offer should not deliver inactive events.
            if (!active(event.status())) {
                continue;
            }

            List<EventMarketDto> eventMarkets = new ArrayList<>();
            Set<String> seenEventMarketIds = new HashSet<>();
            for (EventMarket eventMarket : event.markets()) {
                if (!seenEventMarketIds.add(eventMarket.id())) {
                    throw new IllegalStateException("Duplicate EventMarket ID in Event " + event.id());
                }
                if (!active(eventMarket.status())) {
                    continue;
                }
                Market market = marketById.get(eventMarket.marketId());
                if (market == null || !active(market.status())) {
                    continue; // Never expose an orphan or inactive market reference.
                }

                Map<String, MarketOutcome> marketOutcomeById = new HashMap<>();
                for (MarketOutcome outcome : market.marketOutcomes()) {
                    MarketOutcome previous = marketOutcomeById.putIfAbsent(outcome.id(), outcome);
                    if (previous != null && !previous.equals(outcome)) {
                        throw new IllegalStateException("Conflicting MarketOutcome in Market " + market.id());
                    }
                }

                List<EventMarketOutcomeDto> eventOutcomes = new ArrayList<>();
                Set<String> usedOutcomeIdsForThisMarket = new HashSet<>();
                Set<String> seenEventOutcomeIds = new HashSet<>();
                for (EventMarketOutcome eventOutcome : eventMarket.eventMarketOutcomes()) {
                    if (!seenEventOutcomeIds.add(eventOutcome.id())) {
                        throw new IllegalStateException("Duplicate EventMarketOutcome ID in " + eventMarket.id());
                    }
                    if (!active(eventOutcome.status())) {
                        continue;
                    }
                    MarketOutcome definition = marketOutcomeById.get(eventOutcome.marketOutcomeId());
                    if (definition == null || !active(definition.status())) {
                        continue; // Do not expose an undefined or inactive outcome.
                    }
                    eventOutcomes.add(new EventMarketOutcomeDto(
                            eventOutcome.id(), eventOutcome.marketOutcomeId(),
                            eventOutcome.status().name(), eventOutcome.odds()));
                    usedOutcomeIdsForThisMarket.add(definition.id());
                    putConsistent(usedOutcomes, definition.id(),
                            new OutcomeDto(definition.id(), definition.name(), definition.status().name()));
                }

                eventOutcomes.sort(Comparator.comparing(EventMarketOutcomeDto::id));
                eventMarkets.add(new EventMarketDto(eventMarket.id(), market.id(),
                        eventMarket.status().name(), eventOutcomes));

                // Merge referenced outcome IDs across all EventMarkets sharing one Market.
                MarketDto previous = usedMarkets.get(market.id());
                Set<String> mergedOutcomeIds = new HashSet<>(usedOutcomeIdsForThisMarket);
                if (previous != null) {
                    mergedOutcomeIds.addAll(previous.outcomeIds());
                }
                putConsistentMarket(usedMarkets, new MarketDto(
                        market.id(), market.name(), market.status().name(),
                        mergedOutcomeIds.stream().sorted().toList()));
            }
            eventMarkets.sort(Comparator.comparing(EventMarketDto::id));
            responseEvents.add(new EventDto(event.id(), event.name(), event.startsAt(),
                    event.status().name(), eventMarkets));
        }
        return new OfferResponse(
                responseEvents,
                usedMarkets.values().stream().sorted(Comparator.comparing(MarketDto::id)).toList(),
                usedOutcomes.values().stream().sorted(Comparator.comparing(OutcomeDto::id)).toList());
    }

    private static boolean active(Enum<?> status) {
        return status != null && "ACTIVE".equals(status.name());
    }

    private static <T> void putConsistent(Map<String, T> target, String id, T value) {
        T previous = target.putIfAbsent(id, value);
        if (previous != null && !previous.equals(value)) {
            throw new IllegalStateException("Conflicting outcome definitions for ID: " + id);
        }
    }

    private static void putConsistentMarket(Map<String, MarketDto> target, MarketDto next) {
        MarketDto previous = target.get(next.id());
        if (previous != null && (!previous.name().equals(next.name()) ||
                !previous.status().equals(next.status()))) {
            throw new IllegalStateException("Conflicting Market definitions for ID: " + next.id());
        }
        target.put(next.id(), next);
    }
}
