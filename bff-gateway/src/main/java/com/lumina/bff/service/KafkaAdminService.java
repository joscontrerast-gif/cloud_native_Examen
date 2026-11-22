package com.lumina.bff.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * Servicio dedicado a administrar Kafka (crear, eliminar y listar topics, listar grupos de
 * consumidores). Reemplaza al antiguo RabbitAdminService.
 *
 * <p>Errores de entrada -> {@link IllegalArgumentException} (el controlador responde 400).
 * Errores de comunicación con el broker -> {@link IllegalStateException} (503).</p>
 */
@Service
public class KafkaAdminService {

    private static final Pattern NOMBRE_VALIDO = Pattern.compile("^[a-zA-Z0-9._-]{1,249}$");
    private static final long TIMEOUT_SEGUNDOS = 10;

    private final KafkaAdmin kafkaAdmin;

    public KafkaAdminService(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    /**
     * Crea un topic.
     *
     * @return true si se creó, false si ya existía
     */
    public boolean crearTopic(String nombre, int particiones, short replicas) {
        String topic = validarNombre(nombre);
        try (AdminClient admin = nuevoCliente()) {
            admin.createTopics(List.of(new NewTopic(topic, particiones, replicas)))
                    .all().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS);
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                return false;
            }
            throw errorKafka("crear el topic '" + topic + "'", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw errorKafka("crear el topic '" + topic + "'", e);
        } catch (TimeoutException | KafkaException e) {
            throw errorKafka("crear el topic '" + topic + "'", e);
        }
    }

    /**
     * Elimina un topic.
     *
     * @return true si se eliminó, false si no existía
     */
    public boolean eliminarTopic(String nombre) {
        String topic = validarNombre(nombre);
        if (topic.startsWith("__")) {
            throw new IllegalArgumentException("No se pueden eliminar los topics internos de Kafka.");
        }
        try (AdminClient admin = nuevoCliente()) {
            admin.deleteTopics(List.of(topic)).all().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS);
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return false;
            }
            throw errorKafka("eliminar el topic '" + topic + "'", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw errorKafka("eliminar el topic '" + topic + "'", e);
        } catch (TimeoutException | KafkaException e) {
            throw errorKafka("eliminar el topic '" + topic + "'", e);
        }
    }

    /** Lista los topics (sin los internos de Kafka), ordenados por nombre. */
    public List<String> listarTopics() {
        try (AdminClient admin = nuevoCliente()) {
            return admin.listTopics().names().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS).stream()
                    .filter(nombre -> !nombre.startsWith("__"))
                    .sorted()
                    .toList();
        } catch (ExecutionException e) {
            throw errorKafka("listar los topics", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw errorKafka("listar los topics", e);
        } catch (TimeoutException | KafkaException e) {
            throw errorKafka("listar los topics", e);
        }
    }

    /** Lista los grupos de consumidores registrados (qué servicios están leyendo de Kafka). */
    public List<String> listarGruposConsumidores() {
        try (AdminClient admin = nuevoCliente()) {
            return admin.listConsumerGroups().all().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS).stream()
                    .map(ConsumerGroupListing::groupId)
                    .sorted()
                    .toList();
        } catch (ExecutionException e) {
            throw errorKafka("listar los grupos de consumidores", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw errorKafka("listar los grupos de consumidores", e);
        } catch (TimeoutException | KafkaException e) {
            throw errorKafka("listar los grupos de consumidores", e);
        }
    }

    private AdminClient nuevoCliente() {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    private String validarNombre(String nombre) {
        if (nombre == null || nombre.isBlank()) {
            throw new IllegalArgumentException("El nombre del topic no puede estar vacío.");
        }
        String limpio = nombre.trim();
        if (limpio.equals(".") || limpio.equals("..") || !NOMBRE_VALIDO.matcher(limpio).matches()) {
            throw new IllegalArgumentException(
                    "Nombre de topic inválido: use solo letras, números, '.', '_' o '-' (máximo 249 caracteres).");
        }
        return limpio;
    }

    private IllegalStateException errorKafka(String accion, Exception e) {
        Throwable causa = e.getCause() != null ? e.getCause() : e;
        return new IllegalStateException("No se pudo " + accion + ": " + causa.getMessage(), e);
    }
}
