package com.lumina.bodega.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lumina.bodega.dto.PagoExitosoEvent;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consumidor RabbitMQ de ms-bodega para los pagos exitosos (cola lumina.bodega.queue).
 *
 * <p>El descuento real del stock lo hace el consumidor SQS (PedidoSqsConsumer); aquí no se vuelve
 * a descontar para no descontar dos veces.</p>
 */
@Component
public class PagoExitosoRabbitListener {

    private static final Logger log = LoggerFactory.getLogger(PagoExitosoRabbitListener.class);

    private final ObjectMapper objectMapper;

    public PagoExitosoRabbitListener(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = "${lumina.rabbitmq.queues.bodega:lumina.bodega.queue}")
    public void procesarReservaBodega(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        try {
            PagoExitosoEvent evento = leerEvento(message.getBody());
            log.info("MS-BODEGA (RabbitMQ): Recibido evento de pago | orden={} | monto={}",
                    evento.getOrdenId(), evento.getMonto());

            channel.basicAck(deliveryTag, false);
            log.info("MS-BODEGA (RabbitMQ): Evento de la orden {} procesado con éxito (ACK).", evento.getOrdenId());

        } catch (Exception e) {
            log.error("MS-BODEGA (RabbitMQ): Error al procesar el evento ({}) - enviando a la DLQ mediante NACK.",
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
