package com.lumina.bff.listener;

import com.lumina.bff.config.RabbitMQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Registra en logs los mensajes que llegaron a la Dead Letter Queue de RabbitMQ, es decir, los que
 * ms-bodega o ms-delivery rechazaron (NACK sin reencolar).
 */
@Component
public class PagoDeadLetterRabbitListener {

    private static final Logger log = LoggerFactory.getLogger(PagoDeadLetterRabbitListener.class);

    @RabbitListener(queues = RabbitMQConfig.DLQ_QUEUE)
    public void registrarMensajeNoEntregado(Message message) {
        Map<String, Object> cabeceras = message.getMessageProperties().getHeaders();
        log.error("[DLQ] Mensaje NO entregado | colaOriginal={} | motivo={} | payload={}",
                cabeceras.get("x-first-death-queue"),
                cabeceras.get("x-first-death-reason"),
                new String(message.getBody(), StandardCharsets.UTF_8));
    }
}
