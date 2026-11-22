package com.lumina.bff.service;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.stereotype.Service;

import java.util.regex.Pattern;

/**
 * Administración de RabbitMQ: crear y eliminar colas, exchanges y bindings.
 *
 * <p>Los errores de entrada lanzan {@link IllegalArgumentException} (el controlador responde 400).
 * Los errores del broker llegan como AmqpException y se traducen en el controlador.</p>
 */
@Service
public class RabbitAdminService {

    private static final Pattern NOMBRE_VALIDO = Pattern.compile("^[a-zA-Z0-9._:-]{1,255}$");

    private final AmqpAdmin amqpAdmin;

    public RabbitAdminService(AmqpAdmin amqpAdmin) {
        this.amqpAdmin = amqpAdmin;
    }

    // ---------- Colas ----------

    public void createQueue(String queueName) {
        amqpAdmin.declareQueue(new Queue(validar(queueName, "cola"), true));
    }

    /** @return true si se eliminó, false si no existía */
    public boolean deleteQueue(String queueName) {
        return amqpAdmin.deleteQueue(validar(queueName, "cola"));
    }

    // ---------- Exchanges ----------

    public void createExchange(String name, String type) {
        String nombre = validar(name, "exchange");
        Exchange exchange = switch (type.trim().toLowerCase()) {
            case "direct" -> new DirectExchange(nombre, true, false);
            case "topic" -> new TopicExchange(nombre, true, false);
            case "fanout" -> new FanoutExchange(nombre, true, false);
            case "headers" -> new HeadersExchange(nombre, true, false);
            default -> throw new IllegalArgumentException("Tipo de exchange inválido: " + type);
        };
        amqpAdmin.declareExchange(exchange);
    }

    /** @return true si la operación se ejecutó */
    public boolean deleteExchange(String name) {
        return amqpAdmin.deleteExchange(validar(name, "exchange"));
    }

    // ---------- Bindings ----------

    public void createBinding(String queue, String exchange, String routingKey) {
        amqpAdmin.declareBinding(nuevoBinding(queue, exchange, routingKey));
    }

    public void deleteBinding(String queue, String exchange, String routingKey) {
        amqpAdmin.removeBinding(nuevoBinding(queue, exchange, routingKey));
    }

    private Binding nuevoBinding(String queue, String exchange, String routingKey) {
        return new Binding(validar(queue, "cola"), Binding.DestinationType.QUEUE,
                validar(exchange, "exchange"), routingKey == null ? "" : routingKey.trim(), null);
    }

    private String validar(String valor, String tipo) {
        if (valor == null || valor.isBlank()) {
            throw new IllegalArgumentException("El nombre de la " + tipo + " no puede estar vacío.");
        }
        String limpio = valor.trim();
        if (!NOMBRE_VALIDO.matcher(limpio).matches()) {
            throw new IllegalArgumentException(
                    "Nombre de " + tipo + " inválido: use letras, números, '.', '_', ':' o '-' (máximo 255).");
        }
        return limpio;
    }
}
