#!/usr/bin/env bash
# Muestra el estado del clúster: ensemble de Zookeeper, brokers registrados, topicos y consumer groups.
set -uo pipefail

echo "== Zookeeper (modo de cada nodo: 1 leader + 2 follower) =="
for n in 1 2 3; do
  printf "zookeeper%s: " "$n"
  docker exec zookeeper$n bash -c 'exec 3<>/dev/tcp/localhost/2181; echo srvr >&3; grep Mode <&3' 2>/dev/null || echo "NO RESPONDE"
done

echo
echo "== Brokers registrados en Zookeeper (esperado: [1, 2, 3]) =="
docker exec kafka1 bash -c 'echo "ls /brokers/ids" | zookeeper-shell zookeeper1:2181 2>/dev/null | tail -1'

BOOTSTRAP="kafka1:29092,kafka2:29092,kafka3:29092"
echo
echo "== Topicos =="
docker exec kafka1 kafka-topics --bootstrap-server $BOOTSTRAP --list

echo
echo "== Consumer groups y lag =="
docker exec kafka1 kafka-consumer-groups --bootstrap-server $BOOTSTRAP --all-groups --describe 2>/dev/null || true
