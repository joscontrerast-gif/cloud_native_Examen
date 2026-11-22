package com.lumina.bff.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topología de RabbitMQ (exchanges, colas y bindings) declarada por el BFF, que actúa como
 * microservicio administrador de la mensajería.
 *
 * <p>No hay arranque forzado de la conexión: Spring AMQP declara esta topología automáticamente
 * la primera vez que se conecta a RabbitMQ, y si el broker no está disponible el BFF arranca igual
 * (la conexión se reintenta).</p>
 */
@Configuration
public class RabbitMQConfig {

    // Nombres centralizados de colas y exchanges
    public static final String EXCHANGE_PAGOS = "lumina.pagos.exchange";
    public static final String QUEUE_BODEGA = "lumina.bodega.queue";
    public static final String QUEUE_DELIVERY = "lumina.delivery.queue";
    public static final String ROUTING_KEY_PAGO_EXITOSO = "pago.exitoso";

    // Dead Letter Queue (DLQ)
    public static final String DLQ_EXCHANGE = "lumina.dlq.exchange";
    public static final String DLQ_QUEUE = "lumina.dlq.queue";
    public static final String DLQ_ROUTING_KEY = "dlq.deadletter";

    // 1. Dead Letter Queue
    @Bean
    public DirectExchange dlqExchange() {
        return new DirectExchange(DLQ_EXCHANGE);
    }

    @Bean
    public Queue dlqQueue() {
        return QueueBuilder.durable(DLQ_QUEUE).build();
    }

    @Bean
    public Binding dlqBinding() {
        return BindingBuilder.bind(dlqQueue()).to(dlqExchange()).with(DLQ_ROUTING_KEY);
    }

    // 2. Exchange y colas principales (los mensajes rechazados van a la DLQ)
    @Bean
    public DirectExchange pagosExchange() {
        return new DirectExchange(EXCHANGE_PAGOS);
    }

    @Bean
    public Queue bodegaQueue() {
        return QueueBuilder.durable(QUEUE_BODEGA)
                .withArgument("x-dead-letter-exchange", DLQ_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", DLQ_ROUTING_KEY)
                .build();
    }

    @Bean
    public Queue deliveryQueue() {
        return QueueBuilder.durable(QUEUE_DELIVERY)
                .withArgument("x-dead-letter-exchange", DLQ_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", DLQ_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding bodegaBinding() {
        return BindingBuilder.bind(bodegaQueue()).to(pagosExchange()).with(ROUTING_KEY_PAGO_EXITOSO);
    }

    @Bean
    public Binding deliveryBinding() {
        return BindingBuilder.bind(deliveryQueue()).to(pagosExchange()).with(ROUTING_KEY_PAGO_EXITOSO);
    }
}
