package com.hotelboutique.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public record AumentarParticionesRequest(@NotNull @Min(1) @Max(100) Integer total) {
}
