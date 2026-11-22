package com.lumina.delivery.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lumina.delivery.dto.PagoExitosoEvent;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/** Consumidor RabbitMQ de ms-delivery para los pagos exitosos (cola lumina.delivery.queue). */
@Component
public class PagoExitosoDeliveryRabbitListener {

    private static final Logger log = LoggerFactory.getLogger(PagoExitosoDeliveryRabbitListener.class);

    private final ObjectMapper objectMapper;

    public PagoExitosoDeliveryRabbitListener(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = "${lumina.rabbitmq.queues.delivery:lumina.delivery.queue}")
    public void procesarOrdenDelivery(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        try {
            PagoExitosoEvent evento = leerEvento(message.getBody());
            log.info("MS-DELIVERY (RabbitMQ): Recibido evento de pago | orden={} | monto={}",
                    evento.getOrdenId(), evento.getMonto());

            channel.basicAck(deliveryTag, false);
            log.info("MS-DELIVERY (RabbitMQ): Orden de despacho de la orden {} generada con éxito (ACK).",
                    evento.getOrdenId());

        } catch (Exception e) {
            log.error("MS-DELIVERY (RabbitMQ): Error al generar el despacho ({}) - enviando a la DLQ mediante NACK.",
                    e.getMessage());
            channel.basicNack(deliveryTag, false, false);
        }
    }

    private PagoExitosoEvent leerEvento(byte[] cuerpo) {
        try {
            PagoExitosoEvent evento = objectMapper.readValue(cuerpo, PagoExitosoEvent.class);
            if (evento.getOrdenId() == null || evento.getOrdenId().isBlank()) {
                throw new IllegalArgumentException("El evento no trae ordenId");
            }
            return evento;
        } catch (IOException e) {
            throw new IllegalArgumentException("Mensaje con formato JSON inválido", e);
        }
    }
}
