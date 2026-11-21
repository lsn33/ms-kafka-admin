package com.hotelboutique.kafkaadmin.service;

import com.hotelboutique.kafkaadmin.config.KafkaAdminProperties;
import com.hotelboutique.kafkaadmin.dto.CrearTopicRequest;
import com.hotelboutique.kafkaadmin.dto.ParticionInfo;
import com.hotelboutique.kafkaadmin.dto.ResultadoInicializacion;
import com.hotelboutique.kafkaadmin.dto.TopicDetalle;
import com.hotelboutique.kafkaadmin.dto.TopicResumen;
import com.hotelboutique.kafkaadmin.exception.ConflictoException;
import com.hotelboutique.kafkaadmin.exception.RecursoNoEncontradoException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Service
public class TopicAdminService {

    private static final Logger log = LoggerFactory.getLogger(TopicAdminService.class);

    private final Admin admin;
    private final KafkaAdminProperties props;

    public TopicAdminService(Admin admin, KafkaAdminProperties props) {
        this.admin = admin;
        this.props = props;
    }

    public List<TopicResumen> listar() {
        Set<String> nombres = esperar(admin.listTopics().names(), "listado de topicos");
        if (nombres.isEmpty()) {
            return List.of();
        }
        Map<String, TopicDescription> descripciones =
                esperar(admin.describeTopics(nombres).allTopicNames(), "descripcion de topicos");
        return descripciones.values().stream()
                .map(d -> new TopicResumen(d.name(), d.partitions().size(), replicasPorParticion(d)))
                .sorted(Comparator.comparing(TopicResumen::nombre))
                .toList();
    }

    public TopicDetalle describir(String nombre) {
        Map<String, TopicDescription> descripciones =
                esperar(admin.describeTopics(List.of(nombre)).allTopicNames(), "topico " + nombre);
        TopicDescription descripcion = descripciones.get(nombre);
        if (descripcion == null) {
            throw new RecursoNoEncontradoException("No existe: topico " + nombre);
        }

        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        Map<ConfigResource, Config> configs =
                esperar(admin.describeConfigs(List.of(recurso)).all(), "configuracion del topico " + nombre);

        Map<String, String> configuracion = new TreeMap<>();
        Config config = configs.get(recurso);
        if (config != null) {
            for (ConfigEntry entrada : config.entries()) {
                if (ConfigTopicValidator.CLAVES_PERMITIDAS.contains(entrada.name())) {
                    configuracion.put(entrada.name(), entrada.value());
                }
            }
        }

        List<ParticionInfo> particiones = descripcion.partitions().stream()
                .sorted(Comparator.comparingInt(TopicPartitionInfo::partition))
                .map(p -> new ParticionInfo(
                        p.partition(),
                        p.leader() == null ? null : p.leader().id(),
                        ids(p.replicas()),
                        ids(p.isr())))
                .toList();

        return new TopicDetalle(nombre, particiones, configuracion);
    }

    public TopicDetalle crear(CrearTopicRequest request) {
        ConfigTopicValidator.validar(request.configs());

        NewTopic nuevo = new NewTopic(request.nombre(), request.particiones(), request.replicas().shortValue())
                .configs(request.configs());
        esperar(admin.createTopics(List.of(nuevo)).all(), "topico " + request.nombre());
        log.info("Topico creado: {} ({} particiones, {} replicas)",
                request.nombre(), request.particiones(), request.replicas());
        return describir(request.nombre());
    }

    /** Crea los tópicos definidos en hotel.kafka.topics; los que ya existen se dejan como están. */
    public List<ResultadoInicializacion> inicializarPlantillas() {
        List<ResultadoInicializacion> resultados = new ArrayList<>();
        for (KafkaAdminProperties.TopicPlantilla plantilla : props.topics()) {
            try {
                crear(new CrearTopicRequest(
                        plantilla.nombre(), plantilla.particiones(), plantilla.replicas(), plantilla.configs()));
                resultados.add(new ResultadoInicializacion(plantilla.nombre(), "CREADO"));
            } catch (ConflictoException e) {
                resultados.add(new ResultadoInicializacion(plantilla.nombre(), "YA_EXISTE"));
            }
        }
        return resultados;
    }

    public TopicDetalle actualizarConfiguracion(String nombre, Map<String, String> configs) {
        ConfigTopicValidator.validar(configs);

        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        Collection<AlterConfigOp> operaciones = configs.entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                .toList();
        Map<ConfigResource, Collection<AlterConfigOp>> cambios = Map.of(recurso, operaciones);

        esperar(admin.incrementalAlterConfigs(cambios).all(), "configuracion del topico " + nombre);
        log.info("Configuracion actualizada en {}: {}", nombre, configs);
        return describir(nombre);
    }

    public TopicDetalle aumentarParticiones(String nombre, int total) {
        int actuales = describir(nombre).particiones().size();
        if (total <= actuales) {
            throw new IllegalArgumentException(
                    "Kafka solo permite aumentar particiones: el topico tiene " + actuales + " y se pidio " + total);
        }
        esperar(admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(total))).all(),
                "particiones del topico " + nombre);
        log.info("Particiones de {} aumentadas de {} a {}", nombre, actuales, total);
        return describir(nombre);
    }

    public void eliminar(String nombre) {
        esperar(admin.deleteTopics(List.of(nombre)).all(), "topico " + nombre);
        log.info("Topico eliminado: {}", nombre);
    }

    private <T> T esperar(org.apache.kafka.common.KafkaFuture<T> futuro, String recurso) {
        return AdminFutures.esperar(futuro, props.requestTimeoutMs(), recurso);
    }

    private static int replicasPorParticion(TopicDescription d) {
        return d.partitions().isEmpty() ? 0 : d.partitions().get(0).replicas().size();
    }

    private static List<Integer> ids(List<Node> nodos) {
        return nodos.stream().map(Node::id).toList();
    }
}
