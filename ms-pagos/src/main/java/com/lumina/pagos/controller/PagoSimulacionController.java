package com.lumina.pagos.controller;

import com.lumina.pagos.dto.PagoExitosoEvent;
import com.lumina.pagos.dto.SimularPagoRequest;
import com.lumina.pagos.service.PagoEventosService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/** Recibe el pago simulado del carrito y publica el evento en Kafka y RabbitMQ. */
@RestController
@RequestMapping("/api/pagos")
public class PagoSimulacionController {

    private final PagoEventosService eventosService;

    public PagoSimulacionController(PagoEventosService eventosService) {
        this.eventosService = eventosService;
    }

    @PostMapping("/simular")
    public ResponseEntity<Map<String, Object>> simular(@Valid @RequestBody SimularPagoRequest request) {
        PagoExitosoEvent evento = new PagoExitosoEvent(
                request.ordenId(), request.email(), request.monto(), LocalDateTime.now());

        Map<String, String> resultado = eventosService.publicarPagoExitoso(evento);
        boolean algunoEnviado = resultado.values().stream().anyMatch("enviado"::equals);

        Map<String, Object> cuerpo = new LinkedHashMap<>();
        cuerpo.put("ordenId", request.ordenId());
        cuerpo.putAll(resultado);

        return ResponseEntity
                .status(algunoEnviado ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE)
                .body(cuerpo);
    }
}
