package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class AnalyticsEventConsumer {

    @KafkaListener(topics = "orders", groupId = "analytics-service")
    public void consumeOrder(OrderCreatedEvent event) {
        System.out.println("Analytics Service: pedido creado " + event.getOrderId() + " - total: " + event.getTotal());
    }

    @KafkaListener(topics = "payments", groupId = "analytics-service")
    public void consumePayment(PaymentProcessedEvent event) {
        System.out.println("Analytics Service: pago " + event.getStatus() + " para pedido " + event.getOrderId());
    }

    @KafkaListener(topics = "inventory", groupId = "analytics-service")
    public void consumeInventory(InventoryProcessedEvent event) {
        System.out.println("Analytics Service: inventario " + event.getStatus() + " para pedido " + event.getOrderId());
    }
}
