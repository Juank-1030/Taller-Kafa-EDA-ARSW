package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import edu.eci.arsw.kafka.dto.NotificationSentEvent;
import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import edu.eci.arsw.kafka.producer.NotificationEventProducer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class NotificationEventConsumer {

    private final NotificationEventProducer notificationProducer;

    public NotificationEventConsumer(NotificationEventProducer notificationProducer) {
        this.notificationProducer = notificationProducer;
    }

    @KafkaListener(topics = "payments", groupId = "notification-service")
    public void consumePayment(PaymentProcessedEvent event) {
        System.out.println("Notification Service: pago " + event.getStatus() + " para pedido " + event.getOrderId());

        NotificationSentEvent notification = new NotificationSentEvent(
                "NOT-" + UUID.randomUUID(),
                event.getOrderId(),
                event.getCustomerId(),
                "Pago " + event.getStatus().toLowerCase() + " por $" + event.getTotal(),
                "SENT",
                Instant.now()
        );
        notificationProducer.publish(notification);
        System.out.println("Notification Service: notificación enviada para pedido " + event.getOrderId());
    }

    @KafkaListener(topics = "inventory", groupId = "notification-service")
    public void consumeInventory(InventoryProcessedEvent event) {
        System.out.println("Notification Service: inventario " + event.getStatus() + " para pedido " + event.getOrderId());

        NotificationSentEvent notification = new NotificationSentEvent(
                "NOT-" + UUID.randomUUID(),
                event.getOrderId(),
                event.getCustomerId(),
                "Inventario " + event.getStatus().toLowerCase(),
                "SENT",
                Instant.now()
        );
        notificationProducer.publish(notification);
        System.out.println("Notification Service: notificación enviada para pedido " + event.getOrderId());
    }
}
