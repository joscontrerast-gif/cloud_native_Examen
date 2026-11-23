package com.lumina.bff.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class KafkaGestionAvanzadaServiceTest {

    @Mock
    private KafkaAdmin kafkaAdmin;

    @InjectMocks
    private KafkaGestionAvanzadaService service;

    @Test
    void crearTopicoConConfiguracion_DebeValidarNombreYParametros() {
        assertThatThrownBy(() -> service.crearTopicoConConfiguracion("", 2, (short) 1, Map.of()))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> service.crearTopicoConConfiguracion("topico.test", 0, (short) 1, Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void actualizarConfiguracionTopico_DebeValidarEntrada() {
        assertThatThrownBy(() -> service.actualizarConfiguracionTopico("", Map.of("retention.ms", "1000")))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> service.actualizarConfiguracionTopico("lumina.stock.alertas", Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void eliminarTopico_DebeValidarNombre() {
        assertThatThrownBy(() -> service.eliminarTopico("  "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
