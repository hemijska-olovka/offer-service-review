package com.offer.app.application.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public record OfferResponse(
        List<EventDto> events,
        List<MarketDto> markets,
        List<OutcomeDto> outcomes
) {
    public OfferResponse {
        events = List.copyOf(events);
        markets = List.copyOf(markets);
        outcomes = List.copyOf(outcomes);
    }

    public record EventDto(
            String id,
            String name,
            LocalDateTime startsAt,
            String status,
            List<EventMarketDto> eventMarkets
    ) {
        public EventDto {
            eventMarkets = List.copyOf(eventMarkets);
        }
    }

    public record EventMarketDto(
            String id,
            String marketId,
            String status,
            List<EventMarketOutcomeDto> eventMarketOutcomes
    ) {
        public EventMarketDto {
            eventMarketOutcomes = List.copyOf(eventMarketOutcomes);
        }
    }

    public record EventMarketOutcomeDto(
            String id,
            String outcomeId,
            String status,
            BigDecimal odds
    ) {}

    public record MarketDto(
            String id,
            String name,
            String status,
            List<String> outcomeIds
    ) {
        public MarketDto {
            outcomeIds = List.copyOf(outcomeIds);
        }
    }

    public record OutcomeDto(
            String id,
            String name,
            String status
    ) {}
}
