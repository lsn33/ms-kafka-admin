#!/usr/bin/env bash
# Crea los 2 topicos del proyecto (3 particiones, 3 replicas) desde dentro del contenedor kafka1.
# Es idempotente (--if-not-exists). Alternativa: POST /topics/inicializar en ms-kafka-admin.
set -euo pipefail

BOOTSTRAP="kafka1:29092,kafka2:29092,kafka3:29092"
KT="docker exec kafka1 kafka-topics --bootstrap-server ${BOOTSTRAP}"

# Historial de reservas: se borra lo viejo a los 7 dias (604800000 ms)
$KT --create --if-not-exists --topic hotel.reservas.eventos \
  --partitions 3 --replication-factor 3 \
  --config cleanup.policy=delete --config retention.ms=604800000 --config min.insync.replicas=2

# Estado de usuarios (clave = email): compactado, queda el ultimo valor de cada clave
$KT --create --if-not-exists --topic hotel.usuarios.eventos \
  --partitions 3 --replication-factor 3 \
  --config cleanup.policy=compact --config min.insync.replicas=2 --config min.cleanable.dirty.ratio=0.1

echo
echo "== Topicos creados =="
$KT --describe --topic hotel.reservas.eventos
$KT --describe --topic hotel.usuarios.eventos
