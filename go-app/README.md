# Шаг 0. Равзените кластер какфка 3c-3b.

[Кластер из 6-и нод (3 брокера и 3 контроллера) - Развертывание кластера в Docker 3c-3b](../README.md#развертывание-кластера-в-docker-3c-3b)

# Шаг 1. Создайте топик с 3 партициями и 2 репликами через консоль.

1. Создать топик "topic_unit_1" с 3 партициями и 2 репликами 

```bash
docker exec -it kafka-b-1 kafka-topics --create --topic topic_unit_1 --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2 --config min.insync.replicas=2
```
- `--topic topic_unit_1` - задаем имя топику при создании
- `--bootstrap-server localhost:9092` - указываем параметры подключения к брокеру
  - У нас их 3, можно выбрать любой. В примере указан `kafka-b-1`
- `--partitions 3` - задаем кол-во партиций
- `--replication-factor 2` - задаем кол-во реплик
- `--config min.insync.replicas=2` - глобально задаем минимальное число реплик (в синхронном состоянии), которые должны подтвердить получение сообщения для выполнения успешной записи
2. В результате вывода увидим
```text
Created topic topic_unit_1.
```

3. Зайти в Kafka UI, где так же увидим:
   - [Dashboard](http://localhost:8080)
     - Topics - кол-во топиков 1
     - Partitions - кол-во партиций 3
   - [Brokers](http://localhost:8080/ui/clusters/kafka-kraft/brokers)
     - Partitions - 3 online  
     - Online partitions - у каждого брокера указано 2 
   - [Topics](http://localhost:8080/ui/clusters/kafka-kraft/all-topics)
     - `topic_unit_1` - есть в списке топиков
       - Partitions - 3
       - Replication Factor - 2
- Или через командную строку можем вывести информацию о созданном топике:
```bash
docker exec -it kafka-b-1 kafka-topics --describe --topic topic_unit_1 --bootstrap-server localhost:9092
```
```text
Topic: topic_unit_1	TopicId: 30pgScbCS_CD-LM2Dln_9w	PartitionCount: 3	ReplicationFactor: 2	Configs: min.insync.replicas=2
    Topic: topic_unit_1    Partition: 0    Leader: 6    Replicas: 6,4    Isr: 6,4    Elr:    LastKnownElr: 
    Topic: topic_unit_1    Partition: 1    Leader: 4    Replicas: 4,5    Isr: 4,5    Elr:    LastKnownElr: 
    Topic: topic_unit_1    Partition: 2    Leader: 5    Replicas: 5,6    Isr: 5,6    Elr:    LastKnownElr: 
```

# Шаг 2. Создайте приложение, состоящее из 1 продюсера и 2 консьюмеров.


