# ktobend

Consume kafka data and store it in databend.
### Config
make sure you have a `config.properties` file in the resources dir with the following properties:

```properties
# config.properties
kafka.bootstrap.servers=localhost:9092
kafka.consumer.group.id=1
kafka.json.topic=test_kafka
kafka.file.topic=orders
output.directory=/tmp

databend.dsn=jdbc:databend://tn3ftqihs--medium-p8at.gw.aws-us-east-2.default.databend.com:443?ssl=true
databend.user=cloudapp
databend.password=databend
databend.table=tbcc
databend.batch.size=1
databend.targetTable=tb_t
databend.interval=5

```

## Usage

### How to build
Need Jdk 1.8 or higher version, and maven 3.6.3 or higher version.
First you should have maven installed, then you can run the following command to build the project:
Before you build the project, you should have a databend server running, and write your config in `src/main/resources/config.properties`

```shell
mvn clean package
```
### How to run
```shell
java -jar target/ktobend-1.0-SNAPSHOT-jar-with-dependencies.jar
```

Then you can see:

```shell
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
2 ConsumerJsonWorker and ConsumerStageFileWorker started!
.......
```

### How to test
1. create two table in databend, tmp table and target table
> NOTE: make sure `id`, `batch`, `t` fields are present in the table
```sql
    CREATE TABLE tb_t (
    			id Int64,
                batch String,
    			u64 UInt64,
    			f64 Float64,
    			s   String,
    			s2  String,
    			a16 Array(Int16),
    			a8  Array(UInt8),
    			d   Date,
    			t   DateTime);
```

```sql
    CREATE TABLE tbcc (
    			id Int64,
                batch String,
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

```shell
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties
```

3. run kafka

```shell
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

4. create in json topic

```shell
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test_kafka
```

5. create file info topic

```shell
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic orders
```

6. list topics:

```shell
kafka-topics --list --bootstrap-server localhost:9092
```

7. produce json data to topic

Json data is:

```shell
{"tableName":"tbcc","batch":"2024-03-14-1", "value":{"id":10, "batch":"2024-03-14-1","u64": 30,"f64": 21,"s": "hao","s2": "hello","a16":[1],"a8":[2],"d": "2011-03-06","t": "2016-04-04 12:30:00"}}
```

Value is Array:
```json
{"tableName":"tbcc","batch":"2024-03-14-1", "value":[{"id":10, "batch":"2024-03-14-1","u64": 30,"f64": 22,"s": "hao","s2": "hello","a16":[1],"a8":[2],"d": "2011-03-06","t": "2016-04-04 14:30:00"},{"id":10, "batch":"2024-03-14-1","u64": 30,"f64": 21,"s": "hao","s2": "hello","a16":[1],"a8":[2],"d": "2011-03-06","t": "2016-04-04 12:30:00"}]}
```

```shell
kafka-console-producer --bootstrap-server localhost:9092 --topic test-kafka
```

8. Write  json record will generate a file and upload to stage

![1](https://github.com/hantmac/ktobend/assets/7600925/a110d5d0-18b6-4ab4-957e-1c56ba21a026)

9. The json data will be copied into target table tbcc

![result](https://github.com/hantmac/ktobend/assets/7600925/0fa58c52-fe2f-469a-bd16-226ea6f69baf)
