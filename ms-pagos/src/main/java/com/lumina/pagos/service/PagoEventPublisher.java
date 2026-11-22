package com.lumina.pagos.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lumina.pagos.dto.PagoExitosoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Productor Kafka: publica el evento "pago exitoso" en el topic {@code lumina.pagos.exitoso}.
 *
 * <p>La key del mensaje es el ordenId, de modo que todos los eventos de una misma orden caen en la
 * misma partición y se consumen en orden.</p>
 */
@Service
public class PagoEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(PagoEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public PagoEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                              ObjectMapper objectMapper,
                              @Value("${lumina.kafka.topics.pagos-exitoso:lumina.pagos.exitoso}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public void publicarPagoExitoso(PagoExitosoEvent event) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("No se pudo serializar el evento de pago exitoso", e);
        }

        log.info("Publicando evento de pago exitoso en '{}' para la orden ID: {}", topic, event.getOrdenId());

        kafkaTemplate.send(topic, event.getOrdenId(), payload).whenComplete((resultado, error) -> {
            if (error != null) {
                log.error("No se pudo publicar el evento de la orden {} en Kafka: {}",
                        event.getOrdenId(), error.getMessage());
            } else {
                log.info("Evento de la orden {} publicado | partición={} | offset={}",
                        event.getOrdenId(),
                        resultado.getRecordMetadata().partition(),
                        resultado.getRecordMetadata().offset());
            }
        });
    }
}
