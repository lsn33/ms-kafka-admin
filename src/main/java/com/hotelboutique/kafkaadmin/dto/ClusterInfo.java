package com.hotelboutique.kafkaadmin.dto;

import java.util.List;

public record ClusterInfo(String clusterId, Integer controlador, List<BrokerInfo> brokers) {
}
