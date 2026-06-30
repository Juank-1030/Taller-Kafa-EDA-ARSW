package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NotificationEventConsumer {

    @KafkaListener(topics = "payments", groupId = "notification-service")
    public void consumePayment(PaymentProcessedEvent event) {
        System.out.println("Notification Service: pago " + event.getStatus() + " para pedido " + event.getOrderId());
    }

    @KafkaListener(topics = "inventory", groupId = "notification-service")
    public void consumeInventory(InventoryProcessedEvent event) {
        System.out.println("Notification Service: inventario " + event.getStatus() + " para pedido " + event.getOrderId());
    }
}
