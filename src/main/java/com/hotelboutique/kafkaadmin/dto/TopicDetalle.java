package com.hotelboutique.kafkaadmin.dto;

import java.util.List;
import java.util.Map;

public record TopicDetalle(String nombre, List<ParticionInfo> particiones, Map<String, String> configuracion) {
}
