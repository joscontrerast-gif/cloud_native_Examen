package com.lumina.bff.listener;

import com.lumina.bff.dto.AlertaStockEvent;
import com.lumina.bff.service.AlertaStockMonitorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class AlertaStockKafkaListener {

    private final AlertaStockMonitorService monitorService;

    @KafkaListener(
            topics = "${lumina.kafka.topics.stock-alertas:lumina.stock.alertas}",
            groupId = "bff-alertas-group",
            containerFactory = "alertaStockKafkaListenerContainerFactory"
    )
    public void consumirAlertaStock(
            @Payload AlertaStockEvent evento,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("BFF-GATEWAY [Kafka Consumer]: Mensaje recibido desde particion={}, offset={} -> {}",
                partition, offset, evento);
        monitorService.registrarAlerta(evento);
    }
}
