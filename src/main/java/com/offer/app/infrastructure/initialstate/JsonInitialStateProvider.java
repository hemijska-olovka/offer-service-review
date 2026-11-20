package com.offer.app.infrastructure.initialstate;

import com.offer.app.application.exception.InitialStateLoadException;
import com.offer.app.application.exception.InitialStateNotFoundException;
import com.offer.app.domain.model.Event;
import com.offer.app.domain.model.Market;
import com.offer.app.domain.model.OfferStateSnapshot;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import com.offer.app.domain.model.*;
import com.offer.app.domain.model.status.*;


import java.time.LocalDateTime;
import java.util.List;

public final class JsonInitialStateProvider
        implements InitialStateProvider {

    private final Path marketsPath;
    private final Path eventsPath;
    private final JsonMapper mapper;

    public JsonInitialStateProvider(
            Path marketsPath,
            Path eventsPath,
            JsonMapper mapper
    ) {
        this.marketsPath = Objects.requireNonNull(marketsPath);
        this.eventsPath = Objects.requireNonNull(eventsPath);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public OfferStateSnapshot load() {
        validateFile(marketsPath);
        validateFile(eventsPath);

        try {
            List<Market> markets = readRootArray(
                    marketsPath,
                    this::readMarket
            );

            List<Event> events = readRootArray(
                    eventsPath,
                    this::readEvent
            );

            ensureUniqueIds(
                    markets,
                    Market::id,
                    marketsPath.toString()
            );

            ensureUniqueIds(
                    events,
                    Event::id,
                    eventsPath.toString()
            );

            return new OfferStateSnapshot(events, markets);

        } catch (InitialStateLoadException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw new InitialStateLoadException(
                    "Invalid initial state data: " + ex.getMessage(),
                    ex
            );
        }
    }
    private Market readMarket(JsonNode node, String path){
        requireObject(node, path);

        List<MarketOutcome> outcomes = readArray(
                node,
                "outcomes",
                path,
                this::readMarketOutcome
        );

        ensureUniqueIds(
                outcomes,
                MarketOutcome::id,
                path + ".outcomes"
        );

        return new Market(
                readString(node, "id", path),
                readString(node, "name", path),
                MarketStatus.fromCode(readStatus(node, path)),
                outcomes
        );
    }

    private Event readEvent(JsonNode node, String path) {
        requireObject(node, path);

        List<EventMarket> markets = readArray(
                node,
                "markets",
                path,
                this::readEventMarket
        );

        ensureUniqueIds(
                markets,
                EventMarket::id,
                path + ".markets"
        );

        return new Event(
                readString(node, "id", path),
                readString(node, "name", path),
                LocalDateTime.parse(
                        readString(node, "startsAt", path)
                ),
                EventStatus.fromCode(readStatus(node, path)),
                markets
        );
    }

    private MarketOutcome readMarketOutcome(
            JsonNode node,
            String path
    ) {
        requireObject(node, path);

        return new MarketOutcome(
                readString(node, "id", path),
                readString(node, "name", path),
                MarketOutcomeStatus.fromCode(
                        readStatus(node, path)
                )
        );
    }

    private EventMarket readEventMarket(
            JsonNode node,
            String path
    ) {
        requireObject(node, path);

        List<EventMarketOutcome> outcomes = readArray(
                node,
                "outcomes",
                path,
                this::readEventMarketOutcome
        );

        ensureUniqueIds(
                outcomes,
                EventMarketOutcome::id,
                path + ".outcomes"
        );

        return new EventMarket(
                readString(node, "id", path),
                readString(node, "marketId", path),
                EventMarketStatus.fromCode(
                        readStatus(node, path)
                ),
                outcomes
        );
    }

    private EventMarketOutcome readEventMarketOutcome(
            JsonNode node,
            String path
    ) {
        requireObject(node, path);

        return new EventMarketOutcome(
                readString(node, "id", path),
                readString(node, "outcomeId", path),
                EventMarketOutcomeStatus.fromCode(
                        readStatus(node, path)
                ),
                readDecimal(node, "odds", path)
        );
    }
    private static void requireObject(JsonNode node, String path) {
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException(
                    path + " must be a JSON object"
            );
        }
    }

    private static String readString(
            JsonNode parent,
            String field,
            String path
    ) {
        JsonNode value = parent.get(field);

        if (value == null || value.isNull()) {
            throw new IllegalArgumentException(
                    path + "." + field + " is required"
            );
        }

        if (!value.isString() || value.stringValue().isBlank()) {
            throw new IllegalArgumentException(
                    path + "." + field
                            + " must be a nonblank JSON string"
            );
        }

        return value.stringValue();
    }

    private static <T> void ensureUniqueIds(
            List<T> items,
            Function<T, String> idExtractor,
            String path
    ) {
        Objects.requireNonNull(items, "Items cannot be null");
        Objects.requireNonNull(idExtractor, "ID extractor cannot be null");

        Set<String> ids = new HashSet<>();

        for (T item : items) {
            String id = idExtractor.apply(item);

            if (!ids.add(id)) {
                throw new IllegalArgumentException(
                        "Duplicate ID '" + id + "' at " + path
                );
            }
        }
    }

    private void validateFile(Path path) {
        if (Files.notExists(path)) {
            throw new InitialStateNotFoundException(
                    path.toString()
            );
        }

        if (!Files.isRegularFile(path)) {
            throw new InitialStateLoadException(
                    "Initial state path is not a regular file: " + path
            );
        }
    }
    private static <T> List<T> readArray(
            JsonNode parent,
            String field,
            String path,
            BiFunction<JsonNode, String, T> reader
    ) {
        String arrayPath = path + "." + field;
        JsonNode array = required(parent, field, path);

        if (!array.isArray()) {
            throw new IllegalArgumentException(
                    arrayPath + " must be a JSON array"
            );
        }

        List<T> result = new ArrayList<>();

        for (int index = 0; index < array.size(); index++) {
            String itemPath = arrayPath + "[" + index + "]";

            try {
                result.add(
                        reader.apply(array.get(index), itemPath)
                );
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException(
                        itemPath + ": " + ex.getMessage(),
                        ex
                );
            }
        }

        return List.copyOf(result);
    }

    private static int readStatus(
            JsonNode parent,
            String path
    ) {
        JsonNode value = required(parent, "status", path);

        if (!value.isIntegralNumber()
                || !value.canConvertToInt()) {
            throw new IllegalArgumentException(
                    path + ".status must be an integer"
            );
        }

        return value.intValue();
    }
    private static BigDecimal readDecimal(
            JsonNode parent,
            String field,
            String path
    ) {
        JsonNode value = required(parent, field, path);

        if (!value.isNumber()) {
            throw new IllegalArgumentException(
                    path + "." + field
                            + " must be a JSON number"
            );
        }

        return value.decimalValue();
    }


    private static JsonNode required(
            JsonNode parent,
            String field,
            String path
    ) {
        requireObject(parent, path);

        JsonNode value = parent.get(field);

        if (value == null || value.isNull()) {
            throw new IllegalArgumentException(
                    path + "." + field + " is required"
            );
        }

        return value;
    }
    private <T> List<T> readRootArray(
            Path path,
            BiFunction<JsonNode, String, T> reader
    ) {
        try (var input = Files.newInputStream(path)) {
            JsonNode root = mapper.readTree(input);

            if (root == null || !root.isArray()) {
                throw new IllegalArgumentException(
                        path + " must contain a JSON array"
                );
            }

            List<T> result = new ArrayList<>();

            for (int index = 0; index < root.size(); index++) {
                String itemPath = path + "[" + index + "]";

                try {
                    result.add(
                            reader.apply(root.get(index), itemPath)
                    );
                } catch (RuntimeException ex) {
                    throw new IllegalArgumentException(
                            itemPath + ": " + ex.getMessage(),
                            ex
                    );
                }
            }

            return List.copyOf(result);

        } catch (IOException ex) {
            throw new InitialStateLoadException(
                    "Unable to read file: " + path,
                    ex
            );
        }
    }

}
