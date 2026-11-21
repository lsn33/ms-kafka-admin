package com.hotelboutique.kafkaadmin.service;

import com.hotelboutique.kafkaadmin.config.KafkaAdminProperties;
import com.hotelboutique.kafkaadmin.dto.BrokerInfo;
import com.hotelboutique.kafkaadmin.dto.ClusterInfo;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;

@Service
public class ClusterAdminService {

    private final Admin admin;
    private final KafkaAdminProperties props;

    public ClusterAdminService(Admin admin, KafkaAdminProperties props) {
        this.admin = admin;
        this.props = props;
    }

    public ClusterInfo describir() {
        DescribeClusterResult resultado = admin.describeCluster();
        long timeout = props.requestTimeoutMs();

        String clusterId = AdminFutures.esperar(resultado.clusterId(), timeout, "cluster");
        Node controlador = AdminFutures.esperar(resultado.controller(), timeout, "controlador del cluster");
        List<BrokerInfo> brokers = AdminFutures.esperar(resultado.nodes(), timeout, "brokers").stream()
                .map(n -> new BrokerInfo(n.id(), n.host(), n.port(), n.rack()))
                .sorted(Comparator.comparingInt(BrokerInfo::id))
                .toList();

        return new ClusterInfo(clusterId, controlador == null ? null : controlador.id(), brokers);
    }
}
