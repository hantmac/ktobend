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

![1](https://github.com/hantmac/ktobend/assets/7600925/a110d5d0-18b6-4ab4-957e-1c56ba21a026)

9. The json data will be copied into target table tbcc

![result](https://github.com/hantmac/ktobend/assets/7600925/0fa58c52-fe2f-469a-bd16-226ea6f69baf)
