import javafx.scene.control.Tab;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;


public class flinktest {
    public static void main(String args[]) throws Exception{
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TableEnvironment tableEnv = TableEnvironment.create(fsSettings);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,fsSettings);
        Schema schema = new Schema()
                .field("tag", Types.STRING)
                .field("num", Types.INT)
                .field("timed", Types.STRING)
                .field("name", Types.STRING);

        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("flinktest")
                .property("zookeeper.connec", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
                .startFromLatest()
        ).withSchema(schema)
                .withFormat(new Json()
                .deriveSchema()
        ).inAppendMode()
        .registerTableSource("instance_obj_tb");

        // {"tag": "12345", "num":1715, "name": "pv", "timed": "2017-11-26T01:00:00Z"}
/*       tableEnv.sqlUpdate("CREATE TABLE instance_obj_tb (" +
            "tag VARCHAR,\n" +
            "num INT, \n" +
            "timed VARCHAR, \n" +
            "name VARCHAR \n" +
            ") WITH (\n" +
            "  'connector.type' = 'kafka',\n"+

            "  'connector.version' = 'universal', \n"+

             " 'connector.topic' = 'flinktest', \n"+

             "  'update-mode' = 'upsert',  \n"+

              " 'connector.properties.0.key' = 'zookeeper.connect', \n"+
              " 'connector.properties.0.value' = 'localhost:2181',\n"+
              " 'connector.properties.1.key' = 'bootstrap.servers',\n"+
              " 'connector.properties.1.value' = 'localhost:9092',\n"+
              " 'connector.properties.2.key' = 'group.id',\n" +
              " 'connector.properties.2.value' = 'flink-test',\n" +
              " 'connector.startup-mode' = 'group-offsets' \n" +
              ")"
        );*/
        Table table = tableEnv.sqlQuery("select tag, num, timed, name from instance_obj_tb");
        tableEnv.toAppendStream(table, Row.class).print();

        tableEnv.connect(new Elasticsearch()
                .version("6")
                .host("localhost", 9200, "http")
                .index("flink")
                .documentType("user")
                .keyDelimiter("_")
                .keyNullLiteral("null")
                .failureHandlerFail()
                .disableFlushOnCheckpoint()
                .bulkFlushMaxActions(20)
                // 每个批量请求的缓冲最大值，目前仅支持 MB
                .bulkFlushMaxSize("20 mb")
                // 每个批量请求间隔时间
                .bulkFlushInterval(60000L)
                // 设置刷新批量请求时要使用的常量回退类型
                .bulkFlushBackoffConstant()
                // 设置刷新批量请求时每次回退尝试之间的延迟量（毫秒）
                .bulkFlushBackoffDelay(30000L)
                // 设置刷新批量请求时回退尝试的最大重试次数。
                .bulkFlushBackoffMaxRetries(3)

        ).withFormat(new Json().deriveSchema())
         .withSchema(schema)
         .inUpsertMode()
         .registerTableSink("esflink");

        tableEnv.insertInto(table, "esflink");

        //result.print();
        tableEnv.execute("flink table execute");
    }
}
