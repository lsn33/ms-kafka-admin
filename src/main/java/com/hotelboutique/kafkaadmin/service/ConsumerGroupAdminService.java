package com.hotelboutique.kafkaadmin.service;

import com.hotelboutique.kafkaadmin.config.KafkaAdminProperties;
import com.hotelboutique.kafkaadmin.dto.GrupoDetalle;
import com.hotelboutique.kafkaadmin.dto.GrupoResumen;
import com.hotelboutique.kafkaadmin.dto.MiembroInfo;
import com.hotelboutique.kafkaadmin.dto.OffsetLag;
import com.hotelboutique.kafkaadmin.exception.RecursoNoEncontradoException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ConsumerGroupAdminService {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupAdminService.class);

    private final Admin admin;
    private final KafkaAdminProperties props;

    public ConsumerGroupAdminService(Admin admin, KafkaAdminProperties props) {
        this.admin = admin;
        this.props = props;
    }

    public List<GrupoResumen> listar() {
        Collection<ConsumerGroupListing> grupos =
                esperar(admin.listConsumerGroups().all(), "listado de consumer groups");
        return grupos.stream()
                .map(g -> new GrupoResumen(g.groupId(), g.isSimpleConsumerGroup()))
                .sorted(Comparator.comparing(GrupoResumen::id))
                .toList();
    }

    /** Miembros, offsets confirmados y lag (offset final del log menos offset confirmado) de cada partición. */
    public GrupoDetalle describir(String id) {
        Map<String, ConsumerGroupDescription> descripciones =
                esperar(admin.describeConsumerGroups(List.of(id)).all(), "consumer group " + id);
        ConsumerGroupDescription descripcion = descripciones.get(id);

        Map<TopicPartition, OffsetAndMetadata> confirmados =
                esperar(admin.listConsumerGroupOffsets(id).partitionsToOffsetAndMetadata(), "offsets del grupo " + id);

        Collection<MemberDescription> miembros =
                descripcion == null ? List.of() : descripcion.members();

        // Un grupo sin miembros y sin offsets no existe (Kafka responde con un grupo "muerto")
        if (descripcion == null || (miembros.isEmpty() && confirmados.isEmpty())) {
            throw new RecursoNoEncontradoException("No existe: consumer group " + id);
        }

        List<OffsetLag> offsets = calcularLag(confirmados);
        long lagTotal = offsets.stream().mapToLong(OffsetLag::lag).sum();

        List<MiembroInfo> infoMiembros = miembros.stream()
                .map(m -> new MiembroInfo(
                        m.consumerId(),
                        m.clientId(),
                        m.host(),
                        m.assignment().topicPartitions().stream()
                                .map(tp -> tp.topic() + "-" + tp.partition())
                                .sorted()
                                .toList()))
                .toList();

        String coordinador = descripcion.coordinator() == null
                ? null
                : descripcion.coordinator().host() + ":" + descripcion.coordinator().port();

        return new GrupoDetalle(id, miembros.isEmpty() ? "SIN_MIEMBROS" : "ACTIVO",
                coordinador, infoMiembros, offsets, lagTotal);
    }

    public void eliminar(String id) {
        esperar(admin.deleteConsumerGroups(List.of(id)).all(), "consumer group " + id);
        log.info("Consumer group eliminado: {}", id);
    }

    private List<OffsetLag> calcularLag(Map<TopicPartition, OffsetAndMetadata> confirmados) {
        if (confirmados.isEmpty()) {
            return List.of();
        }
        Map<TopicPartition, OffsetSpec> pedido = new HashMap<>();
        confirmados.keySet().forEach(tp -> pedido.put(tp, OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> finales =
                esperar(admin.listOffsets(pedido).all(), "offsets finales");

        return confirmados.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .map(e -> {
                    TopicPartition tp = e.getKey();
                    long actual = e.getValue().offset();
                    ListOffsetsResult.ListOffsetsResultInfo fin = finales.get(tp);
                    long offsetFinal = fin == null ? actual : fin.offset();
                    return new OffsetLag(tp.topic(), tp.partition(), actual, offsetFinal, Math.max(0, offsetFinal - actual));
                })
                .sorted(Comparator.comparing(OffsetLag::topico).thenComparingInt(OffsetLag::particion))
                .toList();
    }

    private <T> T esperar(KafkaFuture<T> futuro, String recurso) {
        return AdminFutures.esperar(futuro, props.requestTimeoutMs(), recurso);
    }
}
