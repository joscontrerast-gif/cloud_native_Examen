package com.lumina.bodega.messaging;

import com.lumina.bodega.dto.AlertaStockDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class AlertaStockKafkaProducer {

    private final KafkaTemplate<String, AlertaStockDto> alertaStockKafkaTemplate;

    @Value("${lumina.kafka.topics.stock-alertas:lumina.stock.alertas}")
    private String stockAlertasTopic;

    public void publicarAlertaStock(AlertaStockDto alerta) {
        if (alerta == null || alerta.getProductoId() == null) {
            log.warn("MS-BODEGA [Kafka]: Se omitio publicacion de alerta invalida: {}", alerta);
            return;
        }

        String key = String.valueOf(alerta.getProductoId());
        alertaStockKafkaTemplate.send(stockAlertasTopic, key, alerta)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("MS-BODEGA [Kafka]: Error al publicar alerta de stock para producto {} en topico {}: {}",
                                alerta.getProductoId(), stockAlertasTopic, ex.getMessage(), ex);
                    } else {
                        log.info("MS-BODEGA [Kafka]: Alerta de stock publicada -> topico={}, particion={}, offset={}, productoId={}, stockActual={}",
                                stockAlertasTopic,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                alerta.getProductoId(),
                                alerta.getStockActual());
                    }
                });
    }
}
