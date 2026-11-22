package com.lumina.pagos.service;

import com.lumina.pagos.dto.PagoExitosoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Productor RabbitMQ: publica el evento "pago exitoso" en el exchange de pagos. RabbitMQ lo
 * distribuye a las colas de bodega y delivery (tareas de cada microservicio).
 */
@Service
public class PagoRabbitPublisher {

    private static final Logger log = LoggerFactory.getLogger(PagoRabbitPublisher.class);

    private final RabbitTemplate rabbitTemplate;
    private final String exchange;
    private final String routingKey;

    public PagoRabbitPublisher(RabbitTemplate rabbitTemplate,
                               @Value("${lumina.rabbitmq.exchange.pagos:lumina.pagos.exchange}") String exchange,
                               @Value("${lumina.rabbitmq.routing-key.pago-exitoso:pago.exitoso}") String routingKey) {
        this.rabbitTemplate = rabbitTemplate;
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

    public void publicarPagoExitoso(PagoExitosoEvent event) {
        log.info("Publicando evento de pago exitoso en RabbitMQ (exchange '{}') para la orden ID: {}",
                exchange, event.getOrdenId());
        rabbitTemplate.convertAndSend(exchange, routingKey, event);
    }
}
