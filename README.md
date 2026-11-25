# ms-kafka-admin

Microservicio administrador de **Apache Kafka** de AndesStay (DSY1107 · EP5/EP6). Es el equivalente de `ms-rabbit-admin`:
una capa REST para gestionar la plataforma de streaming sin entrar al servidor. Spring Boot 4 · Java 21 · AdminClient de Kafka.

Seguridad: igual que `ms-rabbit-admin`. Todos los endpoints exigen un JWT de Cognito con el grupo `ADMIN`.

## Endpoints

| Método | Ruta | Qué hace |
|---|---|---|
| GET | `/cluster` | Brokers, controlador y clusterId |
| GET | `/topics` | Lista tópicos con particiones y réplicas |
| GET | `/topics/{nombre}` | Detalle: líder, réplicas e ISR de cada partición + configuración |
| POST | `/topics` | Crea un tópico `{nombre, particiones, replicas, configs}` (por defecto 3 y 3) |
| POST | `/topics/inicializar` | Crea los tópicos del proyecto definidos en `application.yml` (idempotente) |
| PUT | `/topics/{nombre}/config` | Modifica configuración (`retention.ms`, `cleanup.policy`, `min.insync.replicas`...) |
| PUT | `/topics/{nombre}/particiones` | Aumenta particiones (Kafka no permite reducirlas) |
| DELETE | `/topics/{nombre}` | Elimina un tópico |
| GET | `/consumer-groups` | Lista consumer groups |
| GET | `/consumer-groups/{id}` | Miembros, partición asignada, offsets y **lag** por partición |
| DELETE | `/consumer-groups/{id}` | Elimina un grupo (solo si no tiene miembros activos) |

Códigos: 201 creado · 204 sin contenido · 400 datos o configuración inválida · 404 no existe · 409 ya existe o grupo activo · 503 Kafka no responde.

Configuraciones permitidas: `cleanup.policy`, `retention.ms`, `retention.bytes`, `min.insync.replicas`, `segment.bytes`, `segment.ms`,
`max.message.bytes`, `compression.type`, `min.cleanable.dirty.ratio`, `delete.retention.ms`.

## Tópicos del proyecto

| Tópico | Particiones / réplicas | Política | Clave | Para qué |
|---|---|---|---|---|
| `hotel.reservas.eventos` | 3 / 3 | `delete`, 7 días, `min.insync.replicas=2` | id de habitación | Historial de eventos de reserva (ocupación, actividad de usuario) |
| `hotel.usuarios.eventos` | 3 / 3 | `compact`, `min.insync.replicas=2` | email | Último estado de cada usuario (copia local en ms-reservas) |

## Ejecutar

```bash
cp .env.example .env            # editar KAFKA_BOOTSTRAP_SERVERS y Cognito
mvn clean test                  # pruebas
mvn clean package -DskipTests   # genera target/ms-kafka-admin-0.0.1-SNAPSHOT.jar
java -jar target/ms-kafka-admin-0.0.1-SNAPSHOT.jar
```

Ejemplos (con un JWT de un usuario del grupo ADMIN):

```bash
TOKEN=eyJ...
curl -H "Authorization: Bearer $TOKEN" http://localhost:8091/cluster
curl -X POST -H "Authorization: Bearer $TOKEN" http://localhost:8091/topics/inicializar
curl -H "Authorization: Bearer $TOKEN" http://localhost:8091/topics/hotel.reservas.eventos
curl -X PUT -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
     -d '{"configs":{"retention.ms":"259200000"}}' http://localhost:8091/topics/hotel.reservas.eventos/config
curl -H "Authorization: Bearer $TOKEN" http://localhost:8091/consumer-groups/reservas-ocupacion
```

## Infraestructura (`infra/`)

- `docker-compose.kafka.yml`: 3 Zookeeper + 3 brokers (Confluent 7.6.1).
- `docker-compose.kafka-ui.yml`: Kafka UI en el puerto 8089 (con login).
- `crear-topicos.sh` y `verificar-cluster.sh`: crear los tópicos por CLI y revisar el estado del clúster.
- `ms-kafka-admin.service`: unidad systemd de ejemplo para la EC2.

```bash
cd infra && cp .env.example .env     # KAFKA_ADVERTISED_HOST = IP privada de la EC2 (o localhost en tu PC)
docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.kafka-ui.yml up -d
./crear-topicos.sh
```
