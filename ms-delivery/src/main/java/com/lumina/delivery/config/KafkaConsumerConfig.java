package com.lumina.delivery.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Configuración de mensajería del consumidor (separada de la lógica de negocio).
 *
 * <ul>
 *   <li>ACK manual: el listener confirma (acknowledge) solo cuando procesó bien el mensaje.</li>
 *   <li>Si falla, se reintenta y, agotados los reintentos, el mensaje se envía al Dead Letter
 *       Topic. Los mensajes inválidos (IllegalArgumentException) van directo al DLT sin reintentos.</li>
 * </ul>
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${lumina.kafka.topics.pagos-dlt:lumina.pagos.exitoso.dlt}")
    private String dltTopic;

    /** Espera entre reintentos, en milisegundos. */
    @Value("${lumina.kafka.retry.interval-ms:1000}")
    private long intervaloReintentoMs;

    /** Reintentos adicionales al primer intento antes de enviar el mensaje al DLT. */
    @Value("${lumina.kafka.retry.max-attempts:2}")
    private long reintentos;

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate, (record, error) -> new TopicPartition(dltTopic, -1));

        DefaultErrorHandler handler = new DefaultErrorHandler(
                recoverer, new FixedBackOff(intervaloReintentoMs, reintentos));
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        // Con ACK manual, confirma el offset del mensaje ya enviado al DLT para no reprocesarlo.
        handler.setCommitRecovered(true);
        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory, DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
