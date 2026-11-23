package com.lumina.bodega.messaging;

import com.lumina.bodega.dto.AlertaStockDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AlertaStockKafkaProducerTest {

    @Mock
    private KafkaTemplate<String, AlertaStockDto> kafkaTemplate;

    @InjectMocks
    private AlertaStockKafkaProducer producer;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(producer, "stockAlertasTopic", "lumina.stock.alertas");
    }

    @Test
    void publicarAlertaStock_DebeEnviarMensajeAlTopico_CuandoAlertaEsValida() {
        AlertaStockDto dto = AlertaStockDto.builder()
                .id(1L)
                .productoId(100L)
                .nombreProducto("Laptop HP")
                .stockActual(4)
                .stockMinimo(10)
                .resuelta(false)
                .build();

        when(kafkaTemplate.send(eq("lumina.stock.alertas"), eq("100"), eq(dto)))
                .thenReturn(new CompletableFuture<>());

        producer.publicarAlertaStock(dto);

        verify(kafkaTemplate, times(1)).send("lumina.stock.alertas", "100", dto);
    }

    @Test
    void publicarAlertaStock_NoDebeEnviar_CuandoAlertaEsNula() {
        producer.publicarAlertaStock(null);
        verify(kafkaTemplate, never()).send(any(), any(), any());
    }
}
