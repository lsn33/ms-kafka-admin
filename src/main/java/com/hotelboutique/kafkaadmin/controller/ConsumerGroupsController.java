package com.hotelboutique.kafkaadmin.controller;

import com.hotelboutique.kafkaadmin.dto.GrupoDetalle;
import com.hotelboutique.kafkaadmin.dto.GrupoResumen;
import com.hotelboutique.kafkaadmin.service.ConsumerGroupAdminService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/consumer-groups")
public class ConsumerGroupsController {

    private final ConsumerGroupAdminService grupoService;

    public ConsumerGroupsController(ConsumerGroupAdminService grupoService) {
        this.grupoService = grupoService;
    }

    @GetMapping
    public List<GrupoResumen> listar() {
        return grupoService.listar();
    }

    @GetMapping("/{id}")
    public GrupoDetalle describir(@PathVariable String id) {
        return grupoService.describir(id);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> eliminar(@PathVariable String id) {
        grupoService.eliminar(id);
        return ResponseEntity.noContent().build();
    }
}
