package com.lumina.bff.controller;

import com.lumina.bff.dto.CrearTopicRequest;
import com.lumina.bff.service.KafkaAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * API de administración de Kafka (solo rol ADMIN, ver SecurityConfig).
 *
 * <ul>
 *   <li>POST   /api/admin/topics</li>
 *   <li>DELETE /api/admin/topics/{name}</li>
 *   <li>GET    /api/admin/topics</li>
 *   <li>GET    /api/admin/consumer-groups</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/admin")
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    public KafkaAdminController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @PostMapping("/topics")
    public ResponseEntity<Map<String, Object>> crearTopic(@Valid @RequestBody CrearTopicRequest request) {
        boolean creado = kafkaAdminService.crearTopic(
                request.name(), request.partitions(), request.replicationFactor());

        if (!creado) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.<String, Object>of("error", "El topic '" + request.name().trim() + "' ya existe."));
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.<String, Object>of(
                "mensaje", "Topic creado exitosamente.",
                "topic", request.name().trim(),
                "partitions", request.partitions(),
                "replicationFactor", request.replicationFactor()));
    }

    @DeleteMapping("/topics/{name}")
    public ResponseEntity<Map<String, Object>> eliminarTopic(@PathVariable("name") String name) {
        boolean eliminado = kafkaAdminService.eliminarTopic(name);

        if (!eliminado) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.<String, Object>of("error", "El topic '" + name + "' no existe."));
        }
        return ResponseEntity.ok(Map.<String, Object>of(
                "mensaje", "Topic eliminado exitosamente.",
                "topic", name));
    }

    @GetMapping("/topics")
    public ResponseEntity<Map<String, Object>> listarTopics() {
        List<String> topics = kafkaAdminService.listarTopics();
        return ResponseEntity.ok(Map.<String, Object>of("total", topics.size(), "topics", topics));
    }

    @GetMapping("/consumer-groups")
    public ResponseEntity<Map<String, Object>> listarGrupos() {
        List<String> grupos = kafkaAdminService.listarGruposConsumidores();
        return ResponseEntity.ok(Map.<String, Object>of("total", grupos.size(), "groups", grupos));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> manejarValidacion(MethodArgumentNotValidException ex) {
        Map<String, String> detalles = new LinkedHashMap<>();
        ex.getBindingResult().getFieldErrors()
                .forEach(error -> detalles.put(error.getField(), error.getDefaultMessage()));
        return ResponseEntity.badRequest()
                .body(Map.<String, Object>of("error", "Parámetros inválidos", "detalles", detalles));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> manejarArgumentoInvalido(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.<String, Object>of("error", ex.getMessage()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, Object>> manejarKafkaNoDisponible(IllegalStateException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.<String, Object>of("error", ex.getMessage()));
    }
}
