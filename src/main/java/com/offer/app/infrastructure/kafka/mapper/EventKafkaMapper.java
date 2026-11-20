
package com.offer.app.infrastructure.kafka.mapper;

import com.offer.app.application.helper.EventUpdateHelper;
import com.offer.app.domain.model.EventMarket;
import com.offer.app.domain.model.EventMarketOutcome;
import com.offer.app.domain.model.status.EventMarketOutcomeStatus;
import com.offer.app.domain.model.status.EventMarketStatus;
import com.offer.app.domain.model.status.EventStatus;

import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Infrastructure boundary:
 * Parses the external Kafka JSON contract
 * into an application update input.
 */
@Component
public final class EventKafkaMapper {

    private final JsonMapper mapper;

    public EventKafkaMapper(JsonMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper);
    }

    public EventUpdateHelper parse(String json) {

        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException(
                    "Event Kafka payload cannot be empty"
            );
        }

        final JsonNode root;

        try {
            root = mapper.readTree(json);
        } catch (JacksonException ex) {
            throw new IllegalArgumentException(
                    "Invalid Event JSON",
                    ex
            );
        }

        requireObject(root, "$");

        return new EventUpdateHelper(
                readString(root, "id", "$"),

                optional(
                        root,
                        "name",
                        node -> readString(node, "$.name")
                ),

                optional(
                        root,
                        "startsAt",
                        node -> parseDate(
                                readString(node, "$.startsAt")
                        )
                ),

                optional(
                        root,
                        "status",
                        node -> EventStatus.fromCode(
                                readStatusCode(node, "$.status")
                        )
                ),

                optional(
                        root,
                        "markets",
                        EventKafkaMapper::parseMarkets
                )
        );
    }

    private static List<EventMarket> parseMarkets(
            JsonNode marketsNode
    ) {

        requireArray(marketsNode, "$.markets");

        List<EventMarket> markets = new ArrayList<>();

        for (int i = 0; i < marketsNode.size(); i++) {

            String path = "$.markets[" + i + "]";

            JsonNode marketNode = marketsNode.get(i);

            requireObject(marketNode, path);

            String id = readString(
                    marketNode,
                    "id",
                    path
            );

            String marketId = readString(
                    marketNode,
                    "marketId",
                    path
            );

            EventMarketStatus status =
                    EventMarketStatus.fromCode(
                            readStatusCode(
                                    required(marketNode, "status", path),
                                    path + ".status"
                            )
                    );

            List<EventMarketOutcome> outcomes =
                    parseOutcomes(marketNode, path);

            markets.add(
                    new EventMarket(
                            id,
                            marketId,
                            status,
                            outcomes
                    )
            );
        }

        return List.copyOf(markets);
    }

    private static List<EventMarketOutcome> parseOutcomes(
            JsonNode marketNode,
            String marketPath
    ) {

        JsonNode outcomesNode = optionalArray(
                marketNode,
                "outcomes",
                marketPath
        );

        // Missing or null outcomes become an empty list.
        if (outcomesNode == null) {
            return List.of();
        }

        List<EventMarketOutcome> outcomes =
                new ArrayList<>();

        for (int i = 0; i < outcomesNode.size(); i++) {

            String path = marketPath + ".outcomes[" + i + "]";

            JsonNode outcomeNode = outcomesNode.get(i);

            requireObject(outcomeNode, path);

            String id = readString(
                    outcomeNode,
                    "id",
                    path
            );

            String outcomeId = readString(
                    outcomeNode,
                    "outcomeId",
                    path
            );

            EventMarketOutcomeStatus status =
                    EventMarketOutcomeStatus.fromCode(
                            readStatusCode(
                                    required(outcomeNode, "status", path),
                                    path + ".status"
                            )
                    );

            BigDecimal odds = readOdds(
                    outcomeNode,
                    path
            );

            outcomes.add(
                    new EventMarketOutcome(
                            id,
                            outcomeId,
                            status,
                            odds
                    )
            );
        }

        return List.copyOf(outcomes);
    }

    private static JsonNode required(
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

        return value;
    }

    private static JsonNode optionalArray(
            JsonNode parent,
            String field,
            String path
    ) {

        JsonNode value = parent.get(field);

        if (value == null || value.isNull()) {
            return null;
        }

        if (!value.isArray()) {
            throw new IllegalArgumentException(
                    path + "." + field + " must be an array"
            );
        }

        return value;
    }

    private static String readString(
            JsonNode parent,
            String field,
            String path
    ) {

        return readString(
                required(parent, field, path),
                path + "." + field
        );
    }

    private static String readString(
            JsonNode node,
            String path
    ) {

        if (node == null || !node.isString()) {
            throw new IllegalArgumentException(
                    path + " must be a string"
            );
        }

        String value = node.stringValue();

        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    path + " must be a nonblank string"
            );
        }

        return value;
    }

    private static int readStatusCode(
            JsonNode node,
            String path
    ) {

        if (node == null
                || !node.isIntegralNumber()
                || !node.canConvertToInt()) {

            throw new IllegalArgumentException(
                    path + " must be an integer status code"
            );
        }

        return node.intValue();
    }

    private static BigDecimal readOdds(
            JsonNode outcomeNode,
            String path
    ) {

        JsonNode oddsNode = required(
                outcomeNode,
                "odds",
                path
        );

        if (!oddsNode.isNumber()) {
            throw new IllegalArgumentException(
                    path + ".odds must be a JSON number"
            );
        }

        return oddsNode.decimalValue();
    }

    private static LocalDateTime parseDate(
            String value
    ) {

        try {
            return LocalDateTime.parse(value);
        } catch (DateTimeParseException ex) {
            throw new IllegalArgumentException(
                    "startsAt must be ISO-8601 local date-time",
                    ex
            );
        }
    }

    private static <T> Optional<T> optional(
            JsonNode object,
            String name,
            Function<JsonNode, T> reader
    ) {

        JsonNode field = object.get(name);

        // Omitted update field: preserve existing value.
        if (field == null) {
            return Optional.empty();
        }

        if (field.isNull()) {
            throw new IllegalArgumentException(
                    "$." + name + " must not be null"
            );
        }

        return Optional.of(reader.apply(field));
    }

    private static void requireObject(
            JsonNode node,
            String path
    ) {

        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException(
                    path + " must be a JSON object"
            );
        }
    }

    private static void requireArray(
            JsonNode node,
            String path
    ) {

        if (node == null || !node.isArray()) {
            throw new IllegalArgumentException(
                    path + " must be a JSON array"
            );
        }
    }
}
