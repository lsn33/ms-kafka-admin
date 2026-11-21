package com.hotelboutique.kafkaadmin.dto;

import jakarta.validation.constraints.NotEmpty;

import java.util.Map;

public record ActualizarConfigRequest(@NotEmpty Map<String, String> configs) {
}
