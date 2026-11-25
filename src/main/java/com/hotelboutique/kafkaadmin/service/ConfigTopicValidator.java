package com.hotelboutique.kafkaadmin.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Valida las configuraciones de tópico que se pueden administrar desde este microservicio. */
public final class ConfigTopicValidator {

    /** Configuraciones permitidas: retención, limpieza, durabilidad y tamaño. */
    public static final Set<String> CLAVES_PERMITIDAS = Set.of(
            "cleanup.policy",
            "retention.ms",
            "retention.bytes",
            "min.insync.replicas",
            "segment.bytes",
            "segment.ms",
            "max.message.bytes",
            "compression.type",
            "min.cleanable.dirty.ratio",
            "delete.retention.ms"
    );

    private static final List<String> POLITICAS_VALIDAS = List.of("delete", "compact", "compact,delete", "delete,compact");

    private ConfigTopicValidator() {
    }

    public static void validar(Map<String, String> configs) {
        if (configs == null) {
            return;
        }
        configs.forEach((clave, valor) -> {
            if (!CLAVES_PERMITIDAS.contains(clave)) {
                throw new IllegalArgumentException("Configuracion no permitida: " + clave
                        + ". Permitidas: " + CLAVES_PERMITIDAS.stream().sorted().toList());
            }
            if (valor == null || valor.isBlank()) {
                throw new IllegalArgumentException("La configuracion " + clave + " no tiene valor");
            }
            switch (clave) {
                case "cleanup.policy" -> {
                    if (!POLITICAS_VALIDAS.contains(valor)) {
                        throw new IllegalArgumentException("cleanup.policy debe ser delete, compact o compact,delete");
                    }
                }
                case "retention.ms", "retention.bytes" -> validarEnteroDesde(clave, valor, -1);
                case "min.insync.replicas" -> validarEnteroDesde(clave, valor, 1);
                case "segment.bytes", "segment.ms", "max.message.bytes", "delete.retention.ms" ->
                        validarEnteroDesde(clave, valor, 1);
                case "min.cleanable.dirty.ratio" -> validarRatio(valor);
                default -> {
                    // compression.type: Kafka valida el valor
                }
            }
        });
    }

    private static void validarEnteroDesde(String clave, String valor, long minimo) {
        try {
            if (Long.parseLong(valor) < minimo) {
                throw new IllegalArgumentException(clave + " debe ser >= " + minimo);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(clave + " debe ser numerico");
        }
    }

    private static void validarRatio(String valor) {
        try {
            double ratio = Double.parseDouble(valor);
            if (ratio < 0 || ratio > 1) {
                throw new IllegalArgumentException("min.cleanable.dirty.ratio debe estar entre 0 y 1");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("min.cleanable.dirty.ratio debe ser numerico");
        }
    }
}
