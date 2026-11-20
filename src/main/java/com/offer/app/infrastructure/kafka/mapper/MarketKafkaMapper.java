
package com.offer.app.infrastructure.kafka.mapper;

import com.offer.app.domain.model.Market;
import com.offer.app.domain.model.MarketOutcome;
import com.offer.app.domain.model.status.MarketOutcomeStatus;
import com.offer.app.domain.model.status.MarketStatus;

import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Component
public final class MarketKafkaMapper {

    private final JsonMapper mapper;

    public MarketKafkaMapper(JsonMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper);
    }

    public Market deserialize(String json) {

        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException(
                    "Market Kafka payload cannot be empty"
            );
        }

        try {
            JsonNode root = mapper.readTree(json);

            requireObject(root, "$");

            String id = readString(root, "id", "$");
            String name = readString(root, "name", "$");

            MarketStatus status = MarketStatus.fromCode(
                    readStatus(root, "$")
            );

            List<MarketOutcome> outcomes =
                    readOutcomes(root);

            return new Market(
                    id,
                    name,
                    status,
                    outcomes
            );

        } catch (JacksonException ex) {
            throw new IllegalArgumentException(
                    "Invalid Market Kafka JSON",
                    ex
            );
        }
    }

    private static List<MarketOutcome> readOutcomes(
            JsonNode root
    ) {

        JsonNode outcomesNode = optionalArray(
                root,
                "outcomes",
                "$"
        );

        if (outcomesNode == null) {
            return List.of();
        }

        List<MarketOutcome> outcomes =
                new ArrayList<>();

        Set<String> outcomeIds =
                new HashSet<>();

        for (int i = 0; i < outcomesNode.size(); i++) {

            String path = "$.outcomes[" + i + "]";

            JsonNode node = outcomesNode.get(i);

            requireObject(node, path);

            String id = readString(
                    node,
                    "id",
                    path
            );

            String name = readString(
                    node,
                    "name",
                    path
            );

            MarketOutcomeStatus status =
                    MarketOutcomeStatus.fromCode(
                            readStatus(node, path)
                    );

            if (!outcomeIds.add(id)) {
                throw new IllegalArgumentException(
                        "Duplicate outcome ID: " + id
                );
            }

            outcomes.add(
                    new MarketOutcome(
                            id,
                            name,
                            status
                    )
            );
        }

        return List.copyOf(outcomes);
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

        // Missing or explicit null means an empty list.
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

        JsonNode value = required(
                parent,
                field,
                path
        );

        if (!value.isString()) {
            throw new IllegalArgumentException(
                    path + "." + field
                            + " must be a string"
            );
        }

        String result = value.stringValue();

        if (result == null || result.isBlank()) {
            throw new IllegalArgumentException(
                    path + "." + field
                            + " must be a nonblank string"
            );
        }

        return result;
    }

    private static int readStatus(
            JsonNode parent,
            String path
    ) {

        JsonNode value = required(
                parent,
                "status",
                path
        );

        if (!value.isIntegralNumber()
                || !value.canConvertToInt()) {

            throw new IllegalArgumentException(
                    path + ".status must be an integer"
            );
        }

        return value.intValue();
    }
}
