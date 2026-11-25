package com.hotelboutique.kafkaadmin.service;

import com.hotelboutique.kafkaadmin.exception.ConflictoException;
import com.hotelboutique.kafkaadmin.exception.KafkaNoDisponibleException;
import com.hotelboutique.kafkaadmin.exception.RecursoNoEncontradoException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Espera el resultado de una operación del AdminClient y traduce los errores de Kafka a errores del microservicio. */
final class AdminFutures {

    private AdminFutures() {
    }

    static <T> T esperar(KafkaFuture<T> futuro, long timeoutMs, String recurso) {
        try {
            return futuro.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaNoDisponibleException("Operacion interrumpida sobre " + recurso, e);
        } catch (TimeoutException e) {
            throw new KafkaNoDisponibleException("Kafka no respondio a tiempo (" + recurso + ")", e);
        } catch (ExecutionException e) {
            throw traducir(e.getCause() == null ? e : e.getCause(), recurso);
        }
    }

    static RuntimeException traducir(Throwable causa, String recurso) {
        if (causa instanceof UnknownTopicOrPartitionException || causa instanceof GroupIdNotFoundException) {
            return new RecursoNoEncontradoException("No existe: " + recurso);
        }
        if (causa instanceof TopicExistsException) {
            return new ConflictoException("Ya existe: " + recurso);
        }
        if (causa instanceof GroupNotEmptyException) {
            return new ConflictoException("El grupo tiene miembros activos: " + recurso);
        }
        if (causa instanceof InvalidTopicException
                || causa instanceof InvalidReplicationFactorException
                || causa instanceof InvalidPartitionsException
                || causa instanceof InvalidConfigurationException
                || causa instanceof InvalidRequestException) {
            return new IllegalArgumentException(causa.getMessage());
        }
        return new KafkaNoDisponibleException("Error de Kafka en " + recurso + ": " + causa.getMessage(), causa);
    }
}
