package com.hotelboutique.kafkaadmin.service;

import com.hotelboutique.kafkaadmin.config.KafkaAdminProperties;
import com.hotelboutique.kafkaadmin.dto.GrupoDetalle;
import com.hotelboutique.kafkaadmin.exception.RecursoNoEncontradoException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConsumerGroupAdminServiceTest {

    private Admin admin;
    private ConsumerGroupAdminService service;

    @BeforeEach
    void setUp() {
        admin = mock(Admin.class);
        service = new ConsumerGroupAdminService(admin,
                new KafkaAdminProperties("localhost:9092", 2000, false, List.of()));
    }

    private void stubGrupo(String id, Collection<MemberDescription> miembros) {
        ConsumerGroupDescription descripcion = mock(ConsumerGroupDescription.class);
        when(descripcion.members()).thenReturn(miembros);
        when(descripcion.coordinator()).thenReturn(new Node(1, "kafka1", 9092));
        DescribeConsumerGroupsResult describe = mock(DescribeConsumerGroupsResult.class);
        when(describe.all()).thenReturn(KafkaFuture.completedFuture(Map.of(id, descripcion)));
        when(admin.describeConsumerGroups(anyCollection())).thenReturn(describe);
    }

    private void stubOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        ListConsumerGroupOffsetsResult resultado = mock(ListConsumerGroupOffsetsResult.class);
        when(resultado.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(offsets));
        when(admin.listConsumerGroupOffsets(anyString())).thenReturn(resultado);
    }

    @Test
    void calculaElLagPorParticionYElTotal() {
        TopicPartition p0 = new TopicPartition("hotel.reservas.eventos", 0);
        TopicPartition p1 = new TopicPartition("hotel.reservas.eventos", 1);

        MemberDescription miembro = mock(MemberDescription.class);
        when(miembro.consumerId()).thenReturn("consumer-1");
        when(miembro.clientId()).thenReturn("reservas-ocupacion-0");
        when(miembro.host()).thenReturn("/172.31.0.25");
        when(miembro.assignment()).thenReturn(new MemberAssignment(Set.of(p0, p1)));
        stubGrupo("reservas-ocupacion", List.of(miembro));

        stubOffsets(Map.of(p0, new OffsetAndMetadata(10), p1, new OffsetAndMetadata(4)));

        ListOffsetsResult finales = mock(ListOffsetsResult.class);
        when(finales.all()).thenReturn(KafkaFuture.completedFuture(Map.of(
                p0, new ListOffsetsResult.ListOffsetsResultInfo(15, 0L, Optional.empty()),
                p1, new ListOffsetsResult.ListOffsetsResultInfo(4, 0L, Optional.empty()))));
        when(admin.listOffsets(anyMap())).thenReturn(finales);

        GrupoDetalle detalle = service.describir("reservas-ocupacion");

        assertEquals("ACTIVO", detalle.estado());
        assertEquals(5, detalle.lagTotal());
        assertEquals(2, detalle.offsets().size());
        assertEquals(5, detalle.offsets().get(0).lag());
        assertEquals(0, detalle.offsets().get(1).lag());
        assertEquals(List.of("hotel.reservas.eventos-0", "hotel.reservas.eventos-1"),
                detalle.miembros().get(0).particiones());
    }

    @Test
    void grupoSinMiembrosNiOffsetsNoExiste() {
        stubGrupo("fantasma", List.of());
        stubOffsets(Map.of());

        assertThrows(RecursoNoEncontradoException.class, () -> service.describir("fantasma"));
    }
}
