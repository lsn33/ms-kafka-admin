package com.hotelboutique.kafkaadmin.controller;

import com.hotelboutique.kafkaadmin.dto.ClusterInfo;
import com.hotelboutique.kafkaadmin.service.ClusterAdminService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/cluster")
public class ClusterController {

    private final ClusterAdminService clusterService;

    public ClusterController(ClusterAdminService clusterService) {
        this.clusterService = clusterService;
    }

    @GetMapping
    public ClusterInfo describir() {
        return clusterService.describir();
    }
}
