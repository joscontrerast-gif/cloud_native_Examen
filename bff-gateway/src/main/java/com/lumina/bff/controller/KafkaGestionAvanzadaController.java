package com.lumina.bff.controller;

import com.lumina.bff.dto.AlertaStockEvent;
import com.lumina.bff.service.AlertaStockMonitorService;
import com.lumina.bff.service.KafkaGestionAvanzadaService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/admin/kafka/gestion")
@RequiredArgsConstructor
public class KafkaGestionAvanzadaController {

    private final KafkaGestionAvanzadaService gestionService;
    private final AlertaStockMonitorService alertaStockMonitorService;

    @GetMapping("/topics")
    public ResponseEntity<?> listarTopicosDetallados() {
        try {
            return ResponseEntity.ok(gestionService.listarTopicosDetallados());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/topics")
    public ResponseEntity<?> crearTopico(
            @RequestParam String nombre,
            @RequestParam(defaultValue = "2") int particiones,
            @RequestParam(defaultValue = "1") short replicas,
            @RequestBody(required = false) Map<String, String> configs) {
        try {
            return ResponseEntity.status(201).body(
                    gestionService.crearTopicoConConfiguracion(nombre, particiones, replicas, configs));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @PutMapping("/topics/{topicName}/config")
    public ResponseEntity<?> actualizarConfigTopico(
            @PathVariable String topicName,
            @RequestBody Map<String, String> configs) {
        try {
            return ResponseEntity.ok(gestionService.actualizarConfiguracionTopico(topicName, configs));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/topics/{topicName}")
    public ResponseEntity<?> eliminarTopico(@PathVariable String topicName) {
        try {
            return ResponseEntity.ok(gestionService.eliminarTopico(topicName));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/consumer-groups")
    public ResponseEntity<?> listarConsumerGroups() {
        try {
            return ResponseEntity.ok(gestionService.listarConsumerGroups());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/alertas-stock")
    public ResponseEntity<List<AlertaStockEvent>> obtenerAlertasStockRecibidas() {
        return ResponseEntity.ok(alertaStockMonitorService.obtenerAlertasRecientes());
    }
}
