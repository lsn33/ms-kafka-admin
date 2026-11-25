package com.hotelboutique.kafkaadmin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;
import java.util.Map;

/** Propiedades "hotel.kafka.*" del application.yml. */
@ConfigurationProperties(prefix = "hotel.kafka")
public record KafkaAdminProperties(
        String bootstrapServers,
        Integer requestTimeoutMs,
        Boolean crearAlIniciar,
        List<TopicPlantilla> topics
) {

    public KafkaAdminProperties {
        if (requestTimeoutMs == null) {
            requestTimeoutMs = 15000;
        }
        if (crearAlIniciar == null) {
            crearAlIniciar = false;
        }
        if (topics == null) {
            topics = List.of();
        }
    }

    /** Definición de un tópico del proyecto (nombre, particiones, réplicas y configuración). */
    public record TopicPlantilla(
            String nombre,
            Integer particiones,
            Integer replicas,
            Map<String, String> configs
    ) {
        public TopicPlantilla {
            if (particiones == null) {
                particiones = 3;
            }
            if (replicas == null) {
                replicas = 3;
            }
            if (configs == null) {
                configs = Map.of();
            }
        }
    }
}
