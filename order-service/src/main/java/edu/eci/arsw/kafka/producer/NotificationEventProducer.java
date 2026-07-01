package edu.eci.arsw.kafka.producer;

import edu.eci.arsw.kafka.dto.NotificationSentEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class NotificationEventProducer {

    private final KafkaTemplate<String, NotificationSentEvent> kafkaTemplate;

    public NotificationEventProducer(KafkaTemplate<String, NotificationSentEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(NotificationSentEvent event) {
        kafkaTemplate.send("notifications", event.getOrderId(), event);
    }
}
