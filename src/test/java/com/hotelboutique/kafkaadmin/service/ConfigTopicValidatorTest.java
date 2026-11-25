package com.hotelboutique.kafkaadmin.service;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfigTopicValidatorTest {

    @Test
    void aceptaConfiguracionesValidas() {
        assertDoesNotThrow(() -> ConfigTopicValidator.validar(Map.of(
                "cleanup.policy", "compact",
                "retention.ms", "604800000",
                "min.insync.replicas", "2",
                "min.cleanable.dirty.ratio", "0.1")));
    }

    @Test
    void aceptaMapaNuloOVacio() {
        assertDoesNotThrow(() -> ConfigTopicValidator.validar(null));
        assertDoesNotThrow(() -> ConfigTopicValidator.validar(Map.of()));
    }

    @Test
    void rechazaClaveNoPermitida() {
        assertThrows(IllegalArgumentException.class,
                () -> ConfigTopicValidator.validar(Map.of("unclean.leader.election.enable", "true")));
    }

    @Test
    void rechazaPoliticaDeLimpiezaInvalida() {
        assertThrows(IllegalArgumentException.class,
                () -> ConfigTopicValidator.validar(Map.of("cleanup.policy", "archive")));
    }

    @Test
    void rechazaValoresNoNumericosOFueraDeRango() {
        assertThrows(IllegalArgumentException.class,
                () -> ConfigTopicValidator.validar(Map.of("retention.ms", "siete-dias")));
        assertThrows(IllegalArgumentException.class,
                () -> ConfigTopicValidator.validar(Map.of("min.insync.replicas", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> ConfigTopicValidator.validar(Map.of("min.cleanable.dirty.ratio", "1.5")));
    }

    @Test
    void permiteRetencionInfinita() {
        assertDoesNotThrow(() -> ConfigTopicValidator.validar(Map.of("retention.ms", "-1")));
    }
}
