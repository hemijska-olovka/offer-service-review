package com.offer.app.application.controller;

import com.offer.app.application.dto.OfferResponse;
import com.offer.app.application.usecase.GetOffersUseCase;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import org.springframework.http.MediaType;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Objects;

@RestController
@RequestMapping(path = "/api/offers", produces = MediaType.APPLICATION_JSON_VALUE)
public final class OfferController {

    private final GetOffersUseCase getOffers;

    public OfferController(GetOffersUseCase getOffers) {
        this.getOffers = Objects.requireNonNull(getOffers);
    }

    @Operation(
            summary = "Get available offers",
            description = """
        Returns all available offers when eventIds is omitted.
        When eventIds is provided, returns only offers
        belonging to the specified events.
        """
    )
    @GetMapping
    public OfferResponse getOffers(
            @Parameter(
                    description = "Optional event IDs",
                    example = "event-001,event-002",
                    required = false
            )
            @RequestParam(
                    name = "eventIds",
                    required = false
            )
            List<String> eventIds
    ) {
        return getOffers.execute(eventIds);
    }
}
