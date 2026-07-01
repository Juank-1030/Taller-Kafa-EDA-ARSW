package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import edu.eci.arsw.kafka.producer.PaymentEventProducer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class PaymentEventConsumer {

    private final PaymentEventProducer paymentProducer;

    public PaymentEventConsumer(PaymentEventProducer paymentProducer) {
        this.paymentProducer = paymentProducer;
    }

    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consume(OrderCreatedEvent event) {
        System.out.println("Payment Service: procesando pago para pedido " + event.getOrderId());

        boolean approved = event.getTotal() <= 250000;

        PaymentProcessedEvent paymentEvent = new PaymentProcessedEvent(
                "PAY-" + UUID.randomUUID(),
                event.getOrderId(),
                event.getCustomerId(),
                event.getTotal(),
                approved ? "APPROVED" : "REJECTED",
                Instant.now()
        );

        paymentProducer.publish(paymentEvent);
        System.out.println("Payment Service: pago " + paymentEvent.getStatus() + " para pedido " + event.getOrderId());
    }
}
