package edu.eci.arsw.kafka.dto;

public class CreateOrderRequest {

    private String customerId;
    private Double total;

    public CreateOrderRequest() {
    }

    public CreateOrderRequest(String customerId, Double total) {
        this.customerId = customerId;
        this.total = total;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public Double getTotal() {
        return total;
    }

    public void setTotal(Double total) {
        this.total = total;
    }
}