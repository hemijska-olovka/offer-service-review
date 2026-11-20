package com.offer.app.infrastructure.notification;

import com.offer.app.application.notification.StateChangeNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public final class StateChangeNotifierImpl
        implements StateChangeNotifier {

    private static final Logger log =
            LoggerFactory.getLogger(StateChangeNotifierImpl.class);
    private final StateChangeDispatcher dispatcher;

    public StateChangeNotifierImpl(
            StateChangeDispatcher dispatcher
    ) {
        this.dispatcher =
                Objects.requireNonNull(dispatcher);
    }
    @Override
    public void marketChanged(String marketId) {
        dispatcher.marketChanged(marketId);
        log.info("Market changed in memory. id={}", marketId);
    }

    @Override
    public void eventChanged(String eventId) {
        dispatcher.eventChanged(eventId);
        log.info("Event changed in memory. id={}", eventId);
    }
}
