package com.hotelboutique.kafkaadmin.config;

import com.hotelboutique.kafkaadmin.service.TopicAdminService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** Crea los tópicos del proyecto al iniciar cuando hotel.kafka.crear-al-iniciar=true. */
@Component
@ConditionalOnProperty(name = "hotel.kafka.crear-al-iniciar", havingValue = "true")
public class TopicInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(TopicInitializer.class);

    private final TopicAdminService topicAdminService;

    public TopicInitializer(TopicAdminService topicAdminService) {
        this.topicAdminService = topicAdminService;
    }

    @Override
    public void run(ApplicationArguments args) {
        try {
            topicAdminService.inicializarPlantillas()
                    .forEach(r -> log.info("Topico {}: {}", r.nombre(), r.estado()));
        } catch (RuntimeException e) {
            // Kafka puede no estar arriba todavía: no se debe caer el microservicio por eso
            log.error("No se pudieron crear los topicos al iniciar: {}", e.getMessage());
        }
    }
}
