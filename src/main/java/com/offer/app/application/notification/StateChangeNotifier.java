package com.offer.app.application.notification;

public interface StateChangeNotifier {
    void marketChanged(String marketId);
    void eventChanged(String eventId);
}
