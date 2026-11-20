package com.offer.app.com.offer.app.infrastructure.kafka.consumer;

import com.offer.app.application.usecase.CreateOrUpdateMarketUseCase;
import com.offer.app.domain.model.Market;
import com.offer.app.infrastructure.kafka.checkpoint.KafkaRecoverySession;
import com.offer.app.infrastructure.kafka.consumer.MarketKafkaConsumer;
import com.offer.app.infrastructure.kafka.mapper.MarketKafkaMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.support.Acknowledgment;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MarketKafkaConsumerTest {

    @Mock
    private CreateOrUpdateMarketUseCase useCase;
    @Mock
    private MarketKafkaMapper mapper;
    @Mock
    private Acknowledgment acknowledgment;
    @Mock
    private ObjectProvider<KafkaRecoverySession> recoverySession;
    @InjectMocks
    private MarketKafkaConsumer consumer;

    private static final String JSON = """
            {
              "id": "3way",
              "name": "3 way",
              "status": 1,
              "outcomes": [
                {"id": "3way-1", "name": "1", "status": 1}
              ]
            }
            """;

    @Test
    void shouldAcknowledgeAfterSuccessfulProcessing() {
        Market market = mock(Market.class);

        when(market.id()).thenReturn("3way");
        when(mapper.deserialize(JSON)).thenReturn(market);

        consumer.consume(record(JSON), acknowledgment);

        InOrder order = inOrder(
                mapper,
                useCase,
                acknowledgment
        );

        order.verify(mapper).deserialize(JSON);
        order.verify(useCase).execute(market);
        order.verify(acknowledgment).acknowledge();
    }

    @Test
    void shouldNotAcknowledgeInvalidJson() {
        when(mapper.deserialize(JSON))
                .thenThrow(new IllegalArgumentException(
                        "Invalid Market JSON"
                ));

        assertThrows(
                IllegalArgumentException.class,
                () -> consumer.consume(
                        record(JSON),
                        acknowledgment
                )
        );

        verifyNoInteractions(useCase, acknowledgment);
    }

    @Test
    void shouldNotAcknowledgeWhenUseCaseFails() {
        Market market = mock(Market.class);

        when(mapper.deserialize(JSON)).thenReturn(market);

        doThrow(new IllegalStateException("Repository failed"))
                .when(useCase)
                .execute(market);

        assertThrows(
                IllegalStateException.class,
                () -> consumer.consume(
                        record(JSON),
                        acknowledgment
                )
        );

        verify(acknowledgment, never()).acknowledge();
    }

    private ConsumerRecord<String, String> record(String json) {
        return new ConsumerRecord<>(
                "market",
                0,
                10L,
                null,
                json
        );
    }
}
