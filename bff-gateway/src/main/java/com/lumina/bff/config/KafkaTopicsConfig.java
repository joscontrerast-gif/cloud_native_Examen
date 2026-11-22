package com.lumina.bff.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declaración centralizada de los topics de Kafka (un bean {@link NewTopic} por caso de uso).
 *
 * <p>Los nombres se pueden cambiar desde application.yml (propiedades {@code lumina.kafka.topics.*});
 * si no se definen se usan los valores por defecto. Spring Kafka crea los topics que falten al
 * arrancar el BFF, que actúa como microservicio administrador de la mensajería.</p>
 */
@Configuration
public class KafkaTopicsConfig {

    /** Pagos exitosos: lo publica ms-pagos y lo consumen ms-bodega y ms-delivery. */
    @Value("${lumina.kafka.topics.pagos-exitoso:lumina.pagos.exitoso}")
    private String topicPagosExitoso;

    /** Dead Letter Topic: mensajes que ningún consumidor logró procesar. */
    @Value("${lumina.kafka.topics.pagos-dlt:lumina.pagos.exitoso.dlt}")
    private String topicPagosDlt;

    @Bean
    public NewTopic pagosExitosoTopic() {
        return TopicBuilder.name(topicPagosExitoso).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic pagosDeadLetterTopic() {
        return TopicBuilder.name(topicPagosDlt).partitions(1).replicas(1).build();
    }
}
