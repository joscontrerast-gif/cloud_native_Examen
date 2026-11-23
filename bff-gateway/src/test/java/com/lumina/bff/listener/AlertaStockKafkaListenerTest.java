package com.lumina.bff.listener;

import com.lumina.bff.dto.AlertaStockEvent;
import com.lumina.bff.service.AlertaStockMonitorService;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AlertaStockKafkaListenerTest {

    private final AlertaStockMonitorService monitorService = new AlertaStockMonitorService();
    private final AlertaStockKafkaListener listener = new AlertaStockKafkaListener(monitorService);

    @Test
    void consumirAlertaStock_DebeRegistrarAlertaEnHistorial_CuandoPayloadEsValido() {
        AlertaStockEvent evento = AlertaStockEvent.builder()
                .id(10L)
                .productoId(5L)
                .nombreProducto("Monitor 27")
                .stockActual(3)
                .stockMinimo(10)
                .resuelta(false)
                .build();

        listener.consumirAlertaStock(evento, 0, 12L);

        assertThat(monitorService.obtenerAlertasRecientes()).hasSize(1);
        assertThat(monitorService.obtenerAlertasRecientes().get(0).getProductoId()).isEqualTo(5L);
    }

    @Test
    void consumirAlertaStock_DebeLanzarExcepcionParaActivarRetryYDLT_CuandoPayloadEsInvalido() {
        AlertaStockEvent invalido = AlertaStockEvent.builder()
                .productoId(null)
                .stockActual(-1)
                .build();

        assertThatThrownBy(() -> listener.consumirAlertaStock(invalido, 0, 13L))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
