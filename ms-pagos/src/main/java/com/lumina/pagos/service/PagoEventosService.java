package com.lumina.pagos.service;

import com.lumina.pagos.dto.PagoExitosoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Publica el evento "pago exitoso" en los dos brokers del proyecto: Kafka (streaming) y RabbitMQ
 * (tareas de bodega y delivery).
 *
 * <p>Cada broker se intenta por separado: si uno está caído, el otro igual recibe el evento y el
 * pago simulado no se interrumpe.</p>
 */
@Service
public class PagoEventosService {

    private static final Logger log = LoggerFactory.getLogger(PagoEventosService.class);

    private final PagoEventPublisher kafkaPublisher;
    private final PagoRabbitPublisher rabbitPublisher;

    public PagoEventosService(PagoEventPublisher kafkaPublisher, PagoRabbitPublisher rabbitPublisher) {
        this.kafkaPublisher = kafkaPublisher;
        this.rabbitPublisher = rabbitPublisher;
    }

    /** @return estado de la publicación por broker ("enviado" o "error: ...") */
    public Map<String, String> publicarPagoExitoso(PagoExitosoEvent evento) {
        Map<String, String> resultado = new LinkedHashMap<>();
        resultado.put("kafka", intentar("Kafka", evento, kafkaPublisher::publicarPagoExitoso));
        resultado.put("rabbitmq", intentar("RabbitMQ", evento, rabbitPublisher::publicarPagoExitoso));
        return resultado;
    }

    private String intentar(String broker, PagoExitosoEvent evento, Consumer<PagoExitosoEvent> accion) {
        try {
            accion.accept(evento);
            return "enviado";
        } catch (Exception e) {
            log.error("No se pudo publicar el evento de la orden {} en {}: {}",
                    evento.getOrdenId(), broker, e.getMessage());
            return "error: " + e.getMessage();
        }
    }
}
