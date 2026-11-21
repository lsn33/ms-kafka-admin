package com.hotelboutique.kafkaadmin.dto;

import java.util.List;

public record MiembroInfo(String id, String clientId, String host, List<String> particiones) {
}
