package com.lumina.bff.controller;

import com.lumina.bff.dto.BindingRequest;
import com.lumina.bff.dto.CrearExchangeRequest;
import com.lumina.bff.service.RabbitAdminService;
import jakarta.validation.Valid;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * API de administración de RabbitMQ (solo rol ADMIN, ver SecurityConfig).
 *
 * <ul>
 *   <li>POST   /api/admin/queues?queueName=...      | DELETE /api/admin/queues/{queueName}</li>
 *   <li>POST   /api/admin/exchanges (JSON)           | DELETE /api/admin/exchanges/{name}</li>
 *   <li>POST   /api/admin/bindings (JSON)            | DELETE /api/admin/bindings?queue=&exchange=&routingKey=</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/admin")
public class RabbitAdminController {

    private final RabbitAdminService rabbitAdminService;

    public RabbitAdminController(RabbitAdminService rabbitAdminService) {
        this.rabbitAdminService = rabbitAdminService;
    }

    // ---------- Colas ----------

    @PostMapping("/queues")
    public ResponseEntity<Map<String, Object>> crearCola(@RequestParam("queueName") String queueName) {
        rabbitAdminService.createQueue(queueName);
        return creado("Cola creada exitosamente.", "queue", queueName.trim());
    }

    @DeleteMapping("/queues/{queueName}")
    public ResponseEntity<Map<String, Object>> eliminarCola(@PathVariable("queueName") String queueName) {
        boolean eliminada = rabbitAdminService.deleteQueue(queueName);
        if (!eliminada) {
            return noExiste("La cola '" + queueName + "' no existe.");
        }
        return ResponseEntity.ok(Map.<String, Object>of("mensaje", "Cola eliminada exitosamente.", "queue", queueName));
    }

    // ---------- Exchanges ----------

    @PostMapping("/exchanges")
    public ResponseEntity<Map<String, Object>> crearExchange(@Valid @RequestBody CrearExchangeRequest request) {
        rabbitAdminService.createExchange(request.name(), request.type());
        return creado("Exchange creado exitosamente.", "exchange", request.name().trim());
    }

    @DeleteMapping("/exchanges/{name}")
    public ResponseEntity<Map<String, Object>> eliminarExchange(@PathVariable("name") String name) {
        rabbitAdminService.deleteExchange(name);
        return ResponseEntity.ok(Map.<String, Object>of("mensaje", "Exchange eliminado.", "exchange", name));
    }

    // ---------- Bindings ----------

    @PostMapping("/bindings")
    public ResponseEntity<Map<String, Object>> crearBinding(@Valid @RequestBody BindingRequest request) {
        rabbitAdminService.createBinding(request.queue(), request.exchange(), request.routingKey());
        return creado("Binding creado exitosamente.", "binding",
                request.exchange().trim() + " -> " + request.queue().trim());
    }

    @DeleteMapping("/bindings")
    public ResponseEntity<Map<String, Object>> eliminarBinding(
            @RequestParam("queue") String queue,
            @RequestParam("exchange") String exchange,
            @RequestParam(value = "routingKey", defaultValue = "") String routingKey) {
        rabbitAdminService.deleteBinding(queue, exchange, routingKey);
        return ResponseEntity.ok(Map.<String, Object>of("mensaje", "Binding eliminado.",
                "binding", exchange + " -> " + queue));
    }

    // ---------- Manejo de errores ----------

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

    @ExceptionHandler(AmqpConnectException.class)
    public ResponseEntity<Map<String, Object>> manejarRabbitNoDisponible(AmqpConnectException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.<String, Object>of("error", "No se pudo conectar con RabbitMQ."));
    }

    @ExceptionHandler(AmqpException.class)
    public ResponseEntity<Map<String, Object>> manejarErrorRabbit(AmqpException ex) {
        return ResponseEntity.badRequest().body(Map.<String, Object>of(
                "error", "RabbitMQ rechazó la operación (verifique que la cola y el exchange existan).",
                "detalle", String.valueOf(ex.getMessage())));
    }

    private ResponseEntity<Map<String, Object>> creado(String mensaje, String clave, String valor) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.<String, Object>of("mensaje", mensaje, clave, valor));
    }

    private ResponseEntity<Map<String, Object>> noExiste(String mensaje) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.<String, Object>of("error", mensaje));
    }
}
