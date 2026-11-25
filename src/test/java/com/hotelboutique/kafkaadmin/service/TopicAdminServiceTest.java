package com.hotelboutique.kafkaadmin.service;

import com.hotelboutique.kafkaadmin.config.KafkaAdminProperties;
import com.hotelboutique.kafkaadmin.dto.CrearTopicRequest;
import com.hotelboutique.kafkaadmin.dto.ResultadoInicializacion;
import com.hotelboutique.kafkaadmin.dto.TopicDetalle;
import com.hotelboutique.kafkaadmin.dto.TopicResumen;
import com.hotelboutique.kafkaadmin.exception.ConflictoException;
import com.hotelboutique.kafkaadmin.exception.RecursoNoEncontradoException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TopicAdminServiceTest {

    private static final String TOPICO = "hotel.reservas.eventos";

    private Admin admin;
    private TopicAdminService service;

    @BeforeEach
    void setUp() {
        admin = mock(Admin.class);
        KafkaAdminProperties props = new KafkaAdminProperties("localhost:9092", 2000, false, List.of(
                new KafkaAdminProperties.TopicPlantilla(TOPICO, 3, 3, Map.of("cleanup.policy", "delete"))));
        service = new TopicAdminService(admin, props);
    }

    private static TopicDescription descripcion(String nombre) {
        Node n1 = new Node(1, "kafka1", 9092);
        Node n2 = new Node(2, "kafka2", 9093);
        Node n3 = new Node(3, "kafka3", 9094);
        List<Node> nodos = List.of(n1, n2, n3);
        return new TopicDescription(nombre, false, List.of(
                new TopicPartitionInfo(0, n1, nodos, nodos),
                new TopicPartitionInfo(1, n2, nodos, nodos),
                new TopicPartitionInfo(2, n3, nodos, nodos)));
    }

    private void stubDescribe(String nombre) {
        DescribeTopicsResult describe = mock(DescribeTopicsResult.class);
        when(describe.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(nombre, descripcion(nombre))));
        when(admin.describeTopics(anyCollection())).thenReturn(describe);

        DescribeConfigsResult configs = mock(DescribeConfigsResult.class);
        Config config = new Config(List.of(
                new ConfigEntry("cleanup.policy", "delete"),
                new ConfigEntry("retention.ms", "604800000"),
                new ConfigEntry("flush.messages", "9999")));
        when(configs.all()).thenReturn(KafkaFuture.completedFuture(
                Map.of(new ConfigResource(ConfigResource.Type.TOPIC, nombre), config)));
        when(admin.describeConfigs(anyCollection())).thenReturn(configs);
    }

    @Test
    void listaTopicosConParticionesYReplicas() {
        ListTopicsResult lista = mock(ListTopicsResult.class);
        when(lista.names()).thenReturn(KafkaFuture.completedFuture(Set.of(TOPICO)));
        when(admin.listTopics()).thenReturn(lista);
        stubDescribe(TOPICO);

        List<TopicResumen> topicos = service.listar();

        assertEquals(1, topicos.size());
        assertEquals(new TopicResumen(TOPICO, 3, 3), topicos.get(0));
    }

    @Test
    void describeTopicoConLiderIsrYSoloConfiguracionesAdministrables() {
        stubDescribe(TOPICO);

        TopicDetalle detalle = service.describir(TOPICO);

        assertEquals(3, detalle.particiones().size());
        assertEquals(1, detalle.particiones().get(0).lider());
        assertEquals(List.of(1, 2, 3), detalle.particiones().get(0).isr());
        assertEquals(Map.of("cleanup.policy", "delete", "retention.ms", "604800000"), detalle.configuracion());
    }

    @Test
    void creaTopicoYLoDescribe() {
        CreateTopicsResult creacion = mock(CreateTopicsResult.class);
        when(creacion.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createTopics(anyCollection())).thenReturn(creacion);
        stubDescribe(TOPICO);

        TopicDetalle detalle = service.crear(new CrearTopicRequest(TOPICO, 3, 3, Map.of("cleanup.policy", "delete")));

        assertEquals(TOPICO, detalle.nombre());
        verify(admin).createTopics(anyCollection());
    }

    @Test
    void noCreaSiLaConfiguracionEsInvalida() {
        CrearTopicRequest invalido = new CrearTopicRequest(TOPICO, 3, 3, Map.of("cleanup.policy", "otra"));

        assertThrows(IllegalArgumentException.class, () -> service.crear(invalido));
    }

    @Test
    void topicoExistenteSeTraduceAConflicto() {
        KafkaFutureImpl<Void> fallo = new KafkaFutureImpl<>();
        fallo.completeExceptionally(new TopicExistsException("ya existe"));
        CreateTopicsResult creacion = mock(CreateTopicsResult.class);
        when(creacion.all()).thenReturn(fallo);
        when(admin.createTopics(anyCollection())).thenReturn(creacion);

        assertThrows(ConflictoException.class,
                () -> service.crear(new CrearTopicRequest(TOPICO, 3, 3, Map.of())));
    }

    @Test
    void inicializarMarcaLosTopicosQueYaExisten() {
        KafkaFutureImpl<Void> fallo = new KafkaFutureImpl<>();
        fallo.completeExceptionally(new TopicExistsException("ya existe"));
        CreateTopicsResult creacion = mock(CreateTopicsResult.class);
        when(creacion.all()).thenReturn(fallo);
        when(admin.createTopics(anyCollection())).thenReturn(creacion);

        List<ResultadoInicializacion> resultados = service.inicializarPlantillas();

        assertEquals(List.of(new ResultadoInicializacion(TOPICO, "YA_EXISTE")), resultados);
    }

    @Test
    void eliminarTopicoInexistenteDevuelveNoEncontrado() {
        KafkaFutureImpl<Void> fallo = new KafkaFutureImpl<>();
        fallo.completeExceptionally(new UnknownTopicOrPartitionException("no existe"));
        DeleteTopicsResult borrado = mock(DeleteTopicsResult.class);
        when(borrado.all()).thenReturn(fallo);
        when(admin.deleteTopics(anyCollection())).thenReturn(borrado);

        assertThrows(RecursoNoEncontradoException.class, () -> service.eliminar("no.existe"));
    }

    @Test
    void noPermiteReducirParticiones() {
        stubDescribe(TOPICO);

        assertThrows(IllegalArgumentException.class, () -> service.aumentarParticiones(TOPICO, 2));
    }
}
