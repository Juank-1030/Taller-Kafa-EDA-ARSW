package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.producer.InventoryEventProducer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class InventoryEventConsumer {

    private final InventoryEventProducer inventoryProducer;

    public InventoryEventConsumer(InventoryEventProducer inventoryProducer) {
        this.inventoryProducer = inventoryProducer;
    }

    @KafkaListener(topics = "orders", groupId = "inventory-service")
    public void consume(OrderCreatedEvent event) {
        System.out.println("Inventory Service: procesando pedido " + event.getOrderId());

        boolean reserved = event.getTotal() <= 300000;

        InventoryProcessedEvent inventoryEvent = new InventoryProcessedEvent(
                "INV-" + UUID.randomUUID(),
                event.getOrderId(),
                event.getCustomerId(),
                reserved ? "RESERVED" : "REJECTED",
                Instant.now()
        );

        inventoryProducer.publish(inventoryEvent);
        System.out.println("Inventory Service: " + inventoryEvent.getStatus() + " para pedido " + event.getOrderId());
    }
}
