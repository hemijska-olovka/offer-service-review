package com.offer.app.infrastructure.initialstate;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Path;

@Configuration
public class InitialStateConfiguration {

    @Bean
    public InitialStateProvider initialStateProvider(
            @Value("${offer.initial-state.markets-path}")
            String marketsPath,

            @Value("${offer.initial-state.events-path}")
            String eventsPath,

            JsonMapper mapper
    ) {
        return new JsonInitialStateProvider(
                Path.of(marketsPath),
                Path.of(eventsPath),
                mapper
        );
    }
}
