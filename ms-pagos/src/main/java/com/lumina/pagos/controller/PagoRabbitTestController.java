package com.lumina.pagos.controller;

import com.lumina.pagos.dto.PagoExitosoEvent;
import com.lumina.pagos.service.PagoRabbitPublisher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** Endpoint de prueba para publicar un pago exitoso en RabbitMQ (el de Kafka sigue aparte). */
@RestController
@RequestMapping("/api/pagos/test/rabbit")
public class PagoRabbitTestController {

    private final PagoRabbitPublisher publisher;

    public PagoRabbitTestController(PagoRabbitPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping("/simular-pago")
    public ResponseEntity<String> simularPago(@RequestParam String ordenId,
                                              @RequestParam String email,
                                              @RequestParam BigDecimal monto) {
        PagoExitosoEvent event = new PagoExitosoEvent(ordenId, email, monto, LocalDateTime.now());
        publisher.publicarPagoExitoso(event);
        return ResponseEntity.ok("Evento de pago exitoso enviado a RabbitMQ para la orden " + ordenId);
    }
}
