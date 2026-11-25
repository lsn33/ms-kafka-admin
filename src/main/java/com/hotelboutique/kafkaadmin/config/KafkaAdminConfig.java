package com.hotelboutique.kafkaadmin.config;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaAdminConfig {

    /** Cliente administrativo de Kafka; se cierra solo al apagar la aplicación. */
    @Bean(destroyMethod = "close")
    public Admin kafkaAdminClient(KafkaAdminProperties props) {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, props.requestTimeoutMs());
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, "ms-kafka-admin");
        return Admin.create(config);
    }
}
