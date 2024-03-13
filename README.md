# ktobend


Consume kafka data and store it in databend.
### Config
```properties
kafka.bootstrap.servers=localhost:9092
kafka.consumer.group.id=1
kafka.json.topic=test_kafka
kafka.url.topic=orders
output.directory=/tmp

databend.dsn=jdbc:databend://tn3ftqihs--medium-p8at.gw.aws-us-east-2.default.databend.com:443?ssl=true
databend.user=cloudapp
databend.password=databend
databend.table=tbcc
databend.batch.size=1
```

### Usage
1. create table in databend

    ```json
    CREATE TABLE tbcc (
    			i64 Int64,
    			u64 UInt64,
    			f64 Float64,
    			s   String,
    			s2  String,
    			a16 Array(Int16),
    			a8  Array(UInt8),
    			d   Date,
    			t   DateTime);
    ```

2. run zookeeper

```json
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties
```

3. run kafka

```json
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

4. create in json topic

```json
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test_kafka
```

5. create file info topic

```json
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic orders
```

6. list topics:

```json
kafka-topics --list --bootstrap-server localhost:9092
```

7. produce json data to topic

Json data is:

```json
{"i64": 10,"u64": 30,"f64": 20,"s": "hao","s2": "hello","a16":[1],"a8":[2],"d": "2011-03-06","t": "2016-04-04 11:30:00"}
```

```json
kafka-console-producer --bootstrap-server localhost:9092 --topic test-kafka
```

8. Write  json record will generate a file and upload to stage

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/cadd555d-a114-4e10-96fa-64b11aa0b5ac/d21137b6-c536-4016-89d4-36a0f9c53fba/Untitled.png)

9. The json data will be copied into target table tbcc

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/cadd555d-a114-4e10-96fa-64b11aa0b5ac/83c899b0-05ec-4e66-988c-112a92d78509/Untitled.png)