package com.hotelboutique.kafkaadmin.dto;

/** Posición del grupo en una partición: offset confirmado, último offset del log y retraso (lag). */
public record OffsetLag(String topico, int particion, long offsetActual, long offsetFinal, long lag) {
}
