package com.lumina.bodega.config;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuración de los consumidores RabbitMQ (separada de la lógica de negocio).
 *
 * <ul>
 *   <li>ACK manual: el listener confirma con basicAck solo si procesó bien el mensaje.</li>
 *   <li>Los mensajes rechazados no se reencolan: van a la Dead Letter Queue.</li>
 *   <li>Si la cola aún no existe, el consumidor no tumba la aplicación: reintenta.</li>
 * </ul>
 */
@Configuration
public class RabbitConsumerConfig {

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            SimpleRabbitListenerContainerFactoryConfigurer configurer, ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setDefaultRequeueRejected(false);
        factory.setMissingQueuesFatal(false);
        return factory;
    }
}
