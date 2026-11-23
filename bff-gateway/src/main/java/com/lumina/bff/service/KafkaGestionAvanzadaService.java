package com.lumina.bff.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class KafkaGestionAvanzadaService {

    private final KafkaAdmin kafkaAdmin;

    public Map<String, Object> crearTopicoConConfiguracion(
            String nombre,
            int particiones,
            short replicas,
            Map<String, String> configs) throws Exception {
        if (nombre == null || nombre.isBlank()) {
            throw new IllegalArgumentException("El nombre del topico es obligatorio");
        }
        if (particiones < 1) {
            throw new IllegalArgumentException("El numero de particiones debe ser mayor o igual a 1");
        }
        if (replicas < 1) {
            throw new IllegalArgumentException("El factor de replicacion debe ser mayor o igual a 1");
        }

        NewTopic nuevoTopico = new NewTopic(nombre.trim(), particiones, replicas);
        if (configs != null && !configs.isEmpty()) {
            nuevoTopico.configs(configs);
        }

        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            client.createTopics(List.of(nuevoTopico)).all().get(10, TimeUnit.SECONDS);
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("topico", nombre.trim());
        resp.put("particiones", particiones);
        resp.put("replicas", replicas);
        resp.put("configuraciones", configs != null ? configs : Map.of());
        resp.put("estado", "CREADO");
        return resp;
    }

    public List<Map<String, Object>> listarTopicosDetallados() throws Exception {
        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Set<String> nombres = client.listTopics().names().get(10, TimeUnit.SECONDS);
            if (nombres.isEmpty()) {
                return List.of();
            }

            Map<String, TopicDescription> descripciones =
                    client.describeTopics(nombres).allTopicNames().get(10, TimeUnit.SECONDS);

            List<ConfigResource> recursos = nombres.stream()
                    .map(n -> new ConfigResource(ConfigResource.Type.TOPIC, n))
                    .toList();
            Map<ConfigResource, Config> configsMap =
                    client.describeConfigs(recursos).all().get(10, TimeUnit.SECONDS);

            return nombres.stream().sorted().map(nombre -> {
                TopicDescription desc = descripciones.get(nombre);
                Config cfg = configsMap.get(new ConfigResource(ConfigResource.Type.TOPIC, nombre));

                Map<String, String> parametrosClave = new LinkedHashMap<>();
                if (cfg != null) {
                    ConfigEntry retention = cfg.get("retention.ms");
                    ConfigEntry cleanup = cfg.get("cleanup.policy");
                    if (retention != null) parametrosClave.put("retention.ms", retention.value());
                    if (cleanup != null) parametrosClave.put("cleanup.policy", cleanup.value());
                }

                Map<String, Object> item = new LinkedHashMap<>();
                item.put("nombre", nombre);
                item.put("particiones", desc != null ? desc.partitions().size() : 0);
                item.put("replicas", (desc != null && !desc.partitions().isEmpty())
                        ? desc.partitions().get(0).replicas().size() : 0);
                item.put("configuraciones", parametrosClave);
                return item;
            }).toList();
        }
    }

    public Map<String, Object> actualizarConfiguracionTopico(String topicName, Map<String, String> nuevasConfigs) throws Exception {
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("El nombre del topico es obligatorio");
        }
        if (nuevasConfigs == null || nuevasConfigs.isEmpty()) {
            throw new IllegalArgumentException("Debe enviar al menos un parametro de configuracion (ej. retention.ms, cleanup.policy)");
        }

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName.trim());
        Collection<AlterConfigOp> ops = nuevasConfigs.entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                .toList();

        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            client.incrementalAlterConfigs(Map.of(resource, ops)).all().get(10, TimeUnit.SECONDS);
        }

        return Map.of(
                "topico", topicName.trim(),
                "configuracionesActualizadas", nuevasConfigs,
                "estado", "ACTUALIZADO"
        );
    }

    public Map<String, Object> eliminarTopico(String topicName) throws Exception {
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("El nombre del topico es obligatorio");
        }
        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            client.deleteTopics(List.of(topicName.trim())).all().get(10, TimeUnit.SECONDS);
        }
        return Map.of("topico", topicName.trim(), "estado", "ELIMINADO");
    }

    public List<Map<String, Object>> listarConsumerGroups() throws Exception {
        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Collection<ConsumerGroupListing> listings = client.listConsumerGroups().all().get(10, TimeUnit.SECONDS);
            List<String> groupIds = listings.stream().map(ConsumerGroupListing::groupId).toList();

            if (groupIds.isEmpty()) {
                return List.of();
            }

            Map<String, ConsumerGroupDescription> descriptions =
                    client.describeConsumerGroups(groupIds).all().get(10, TimeUnit.SECONDS);

            return descriptions.values().stream().map(desc -> {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("groupId", desc.groupId());
                item.put("estado", desc.state().toString());
                item.put("miembrosActivos", desc.members().size());
                item.put("topicosAsignados", desc.members().stream()
                        .flatMap(m -> m.assignment().topicPartitions().stream())
                        .map(tp -> tp.topic())
                        .distinct()
                        .toList());
                return item;
            }).toList();
        }
    }
}
