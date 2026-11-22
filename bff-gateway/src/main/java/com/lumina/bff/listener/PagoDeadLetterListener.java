package com.lumina.bff.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Registra en logs los mensajes que llegaron al Dead Letter Topic, es decir, los que ms-bodega o
 * ms-delivery no lograron procesar tras agotar los reintentos (o que eran inválidos).
 */
@Component
public class PagoDeadLetterListener {

    private static final Logger log = LoggerFactory.getLogger(PagoDeadLetterListener.class);

    @KafkaListener(
            topics = "${lumina.kafka.topics.pagos-dlt:lumina.pagos.exitoso.dlt}",
            groupId = "${lumina.kafka.groups.dlt:bff-dlt-group}",
            properties = "auto.offset.reset=earliest")
    public void registrarMensajeNoEntregado(ConsumerRecord<String, String> record) {
        log.error("[DLT] Mensaje NO entregado | topicOriginal={} | consumidor={} | key={} | error={} | payload={}",
                header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                header(record, "kafka_dlt-original-consumer-group"),
                record.key(),
                header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                record.value());
    }

    private String header(ConsumerRecord<String, String> record, String nombre) {
        Header header = record.headers().lastHeader(nombre);
        return header == null ? "n/a" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
