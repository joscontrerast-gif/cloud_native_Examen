package com.lumina.delivery.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lumina.delivery.dto.PagoExitosoEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumidor Kafka de ms-delivery para los pagos exitosos (grupo "delivery-group").
 * Cada grupo recibe todos los eventos del topic, igual que antes cada cola recibía una copia.
 */
@Component
public class PagoExitosoDeliveryListener {

    private static final Logger log = LoggerFactory.getLogger(PagoExitosoDeliveryListener.class);

    private final ObjectMapper objectMapper;

    public PagoExitosoDeliveryListener(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${lumina.kafka.topics.pagos-exitoso:lumina.pagos.exitoso}",
            groupId = "${lumina.kafka.groups.delivery:delivery-group}",
            properties = "auto.offset.reset=earliest")
    public void procesarOrdenDelivery(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            PagoExitosoEvent evento = leerEvento(record.value());
            log.info("MS-DELIVERY: Recibido evento de pago | orden={} | monto={} | partición={} | offset={}",
                    evento.getOrdenId(), evento.getMonto(), record.partition(), record.offset());

            // ACK explícito: solo se confirma si el procesamiento terminó bien.
            ack.acknowledge();
            log.info("MS-DELIVERY: Orden de despacho de la orden {} generada con éxito (ACK).", evento.getOrdenId());

        } catch (RuntimeException e) {
            // Se relanza: el DefaultErrorHandler reintenta y, si se agotan los reintentos
            // (o el mensaje es inválido), lo envía al Dead Letter Topic.
            log.error("MS-DELIVERY: Error al generar el despacho (offset {}): {}", record.offset(), e.getMessage());
            throw e;
        }
    }

    private PagoExitosoEvent leerEvento(String json) {
        try {
            PagoExitosoEvent evento = objectMapper.readValue(json, PagoExitosoEvent.class);
            if (evento.getOrdenId() == null || evento.getOrdenId().isBlank()) {
                throw new IllegalArgumentException("El evento no trae ordenId");
            }
            return evento;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Mensaje con formato JSON inválido: " + e.getOriginalMessage(), e);
        }
    }
}
