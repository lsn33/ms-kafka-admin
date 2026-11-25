package com.hotelboutique.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.util.Map;

public record CrearTopicRequest(
        @NotBlank
        @Size(max = 249)
        @Pattern(regexp = "[a-zA-Z0-9._-]+", message = "solo letras, numeros, punto, guion y guion bajo")
        String nombre,
        @Min(1) @Max(100) Integer particiones,
        @Min(1) @Max(10) Integer replicas,
        Map<String, String> configs
) {

    public CrearTopicRequest {
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
