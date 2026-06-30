package edu.eci.arsw.kafka.producer;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class InventoryEventProducer {

    private final KafkaTemplate<String, InventoryProcessedEvent> kafkaTemplate;

    public InventoryEventProducer(KafkaTemplate<String, InventoryProcessedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(InventoryProcessedEvent event) {
        kafkaTemplate.send("inventory", event.getOrderId(), event);
    }
}
