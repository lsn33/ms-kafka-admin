package com.hotelboutique.kafkaadmin.controller;

import com.hotelboutique.kafkaadmin.dto.ParticionInfo;
import com.hotelboutique.kafkaadmin.dto.TopicDetalle;
import com.hotelboutique.kafkaadmin.dto.TopicResumen;
import com.hotelboutique.kafkaadmin.exception.ConflictoException;
import com.hotelboutique.kafkaadmin.exception.GlobalExceptionHandler;
import com.hotelboutique.kafkaadmin.exception.RecursoNoEncontradoException;
import com.hotelboutique.kafkaadmin.service.TopicAdminService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class TopicsControllerTest {

    private TopicAdminService service;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        service = mock(TopicAdminService.class);
        mvc = MockMvcBuilders.standaloneSetup(new TopicsController(service))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void listaTopicos() throws Exception {
        when(service.listar()).thenReturn(List.of(new TopicResumen("hotel.reservas.eventos", 3, 3)));

        mvc.perform(get("/topics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].nombre").value("hotel.reservas.eventos"))
                .andExpect(jsonPath("$[0].particiones").value(3))
                .andExpect(jsonPath("$[0].replicas").value(3));
    }

    @Test
    void creaTopicoConValoresPorDefecto() throws Exception {
        when(service.crear(any())).thenReturn(new TopicDetalle("demo", List.of(
                new ParticionInfo(0, 1, List.of(1, 2, 3), List.of(1, 2, 3))), Map.of()));

        mvc.perform(post("/topics").contentType(MediaType.APPLICATION_JSON).content("{\"nombre\":\"demo\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.nombre").value("demo"));
    }

    @Test
    void rechazaNombreInvalido() throws Exception {
        mvc.perform(post("/topics").contentType(MediaType.APPLICATION_JSON).content("{\"nombre\":\"con espacios\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void topicoExistenteDevuelve409() throws Exception {
        when(service.crear(any())).thenThrow(new ConflictoException("Ya existe: topico demo"));

        mvc.perform(post("/topics").contentType(MediaType.APPLICATION_JSON).content("{\"nombre\":\"demo\"}"))
                .andExpect(status().isConflict());
    }

    @Test
    void describirInexistenteDevuelve404() throws Exception {
        when(service.describir("nada")).thenThrow(new RecursoNoEncontradoException("No existe: topico nada"));

        mvc.perform(get("/topics/nada")).andExpect(status().isNotFound());
    }

    @Test
    void eliminaTopico() throws Exception {
        mvc.perform(delete("/topics/demo")).andExpect(status().isNoContent());
    }

    @Test
    void eliminarInexistenteDevuelve404() throws Exception {
        doThrow(new RecursoNoEncontradoException("No existe")).when(service).eliminar("nada");

        mvc.perform(delete("/topics/nada")).andExpect(status().isNotFound());
    }
}
