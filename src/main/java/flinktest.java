import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.util.Collector;


public class flinktest {
    public static void main(String[] args) throws Exception{
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

        // {"tag": "zookeeper", "num":512, "name": "pv", "timed": "2017-11-26T01:00:00Z"}
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
        tableEnv.sqlUpdate("create table sql_table( "+
                "id INT, \n" +
                "tag VARCHAR,\n" +
                "num INT, \n" +
                "timed VARCHAR, \n" +
                "name VARCHAR \n" +
                ") WITH (\n" +
                "'connector.type' = 'jdbc',\n"+
                "'connector.url' = 'jdbc:mysql://localhost:3306/flink?useSSL=false',\n" +
                "'connector.table' = 'flink_test', \n" +
                "'connector.driver' = 'com.mysql.cj.jdbc.Driver', \n" +
                "'connector.username' = 'root', \n" +
                "'connector.password' = '123456', \n" +
                " 'connector.write.flush.max-rows' = '5000', \n" +
                "'connector.write.flush.interval' = '2s',\n" +
                "'connector.write.max-retries' = '3'\n" +
                ")"
        );

        Table table = tableEnv.sqlQuery("select tag, num, timed, name from instance_obj_tb");
        //tableEnv.toAppendStream(table, InstanceObj.class).print();

        //tableEnv.insertInto(table, "sql_table");
        //table = tableEnv.sqlQuery("select id, tag, num ,timed, name from sql_table");
        DataStream<InstanceObj> obj = tableEnv.toAppendStream(table, InstanceObj.class);
         SingleOutputStreamOperator<Tuple5<Integer, String, Integer, String, String>> ds = obj.process(new ProcessFunction<InstanceObj, Tuple5<Integer,String,Integer,String,String>>() {
             @Override
             public void processElement(InstanceObj instanceObj, Context context, Collector<Tuple5<Integer, String, Integer, String, String>> collector) throws Exception {
                 SQLTB tb = new SQLTB(5, instanceObj.tag, instanceObj.num, instanceObj.timed, instanceObj.name);
                 collector.collect(tb.toTuple());
             }
        });
        table = tableEnv.fromDataStream(ds);
        tableEnv.insertInto(table, "sql_table");
        //tableEnv.registerTable("sql_table_flink", table);
        //table = tableEnv.sqlQuery("select id,  tag, num, timed, name from sql_table");
        //tableEnv.toAppendStream(table, Row.class).print();
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
         .inAppendMode()
         .registerTableSink("esflink");

        //tableEnv.insertInto(table, "esflink");

        //table = tableEnv.sqlQuery("select t1.id, t1.tag, t1.num, t1.timed, t1.name from sql_table t1 inner join instance_obj_tb t2 on t1.num = t2.num ");
        //table = tableEnv.sqlQuery("select t1.tag, t1.num from sql_table t1 inner join instance_obj_tb t2 on t1.num = t2.num ");
        //tableEnv.toAppendStream(table, Row.class).print();
        //result.print();
        tableEnv.execute("flink table execute");
    }
}
