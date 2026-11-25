package com.hotelboutique.kafkaadmin.dto;

import java.util.List;

/** Estado de una partición: broker líder, réplicas asignadas y réplicas en sincronía (ISR). */
public record ParticionInfo(int particion, Integer lider, List<Integer> replicas, List<Integer> isr) {
}
