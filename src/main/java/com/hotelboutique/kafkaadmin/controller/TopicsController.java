package com.hotelboutique.kafkaadmin.controller;

import com.hotelboutique.kafkaadmin.dto.ActualizarConfigRequest;
import com.hotelboutique.kafkaadmin.dto.AumentarParticionesRequest;
import com.hotelboutique.kafkaadmin.dto.CrearTopicRequest;
import com.hotelboutique.kafkaadmin.dto.ResultadoInicializacion;
import com.hotelboutique.kafkaadmin.dto.TopicDetalle;
import com.hotelboutique.kafkaadmin.dto.TopicResumen;
import com.hotelboutique.kafkaadmin.service.TopicAdminService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/topics")
public class TopicsController {

    private final TopicAdminService topicAdminService;

    public TopicsController(TopicAdminService topicAdminService) {
        this.topicAdminService = topicAdminService;
    }

    @GetMapping
    public List<TopicResumen> listar() {
        return topicAdminService.listar();
    }

    @GetMapping("/{nombre}")
    public TopicDetalle describir(@PathVariable String nombre) {
        return topicAdminService.describir(nombre);
    }

    @PostMapping
    public ResponseEntity<TopicDetalle> crear(@Valid @RequestBody CrearTopicRequest request) {
        return ResponseEntity.status(201).body(topicAdminService.crear(request));
    }

    /** Crea los tópicos del proyecto definidos en application.yml (idempotente). */
    @PostMapping("/inicializar")
    public List<ResultadoInicializacion> inicializar() {
        return topicAdminService.inicializarPlantillas();
    }

    @PutMapping("/{nombre}/config")
    public TopicDetalle actualizarConfiguracion(@PathVariable String nombre,
                                                @Valid @RequestBody ActualizarConfigRequest request) {
        return topicAdminService.actualizarConfiguracion(nombre, request.configs());
    }

    @PutMapping("/{nombre}/particiones")
    public TopicDetalle aumentarParticiones(@PathVariable String nombre,
                                            @Valid @RequestBody AumentarParticionesRequest request) {
        return topicAdminService.aumentarParticiones(nombre, request.total());
    }

    @DeleteMapping("/{nombre}")
    public ResponseEntity<Void> eliminar(@PathVariable String nombre) {
        topicAdminService.eliminar(nombre);
        return ResponseEntity.noContent().build();
    }
}
