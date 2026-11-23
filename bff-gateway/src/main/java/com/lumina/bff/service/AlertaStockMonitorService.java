package com.lumina.bff.service;

import com.lumina.bff.dto.AlertaStockEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

@Service
@Slf4j
public class AlertaStockMonitorService {

    private static final int MAX_HISTORIAL = 100;
    private final Deque<AlertaStockEvent> alertasRecibidas = new ConcurrentLinkedDeque<>();

    public void registrarAlerta(AlertaStockEvent evento) {
        if (evento == null || evento.getProductoId() == null || evento.getStockActual() == null) {
            throw new IllegalArgumentException("Payload de AlertaStockEvent invalido o incompleto");
        }
        if (evento.getStockActual() < 0) {
            throw new IllegalArgumentException("Stock actual no puede ser negativo: " + evento.getStockActual());
        }

        alertasRecibidas.addFirst(evento);
        while (alertasRecibidas.size() > MAX_HISTORIAL) {
            alertasRecibidas.removeLast();
        }

        log.info("BFF-GATEWAY [Dominio]: Alerta de reposicion registrada para producto '{}' (ID: {}) - Stock: {}/{}",
                evento.getNombreProducto(), evento.getProductoId(), evento.getStockActual(), evento.getStockMinimo());
    }

    public List<AlertaStockEvent> obtenerAlertasRecientes() {
        return new ArrayList<>(alertasRecibidas);
    }
}
