package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderEventConsumer {

    @KafkaListener(topics = "orders", groupId = "inventory-service")
    public void consume(OrderCreatedEvent event) {
        System.out.println("Evento recibido en inventory-service: " + event.getOrderId());
    }
}