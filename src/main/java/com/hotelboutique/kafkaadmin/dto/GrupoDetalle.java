package com.hotelboutique.kafkaadmin.dto;

import java.util.List;

public record GrupoDetalle(
        String id,
        String estado,
        String coordinador,
        List<MiembroInfo> miembros,
        List<OffsetLag> offsets,
        long lagTotal
) {
}
