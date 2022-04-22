package app;

import com.alibaba.ververica.cdc.connectors.postgres.PostgreSQLSource;
import fun.MyStringDebeziumDeserializationSchema;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import utils.MyKafkaUtil;

import java.util.Properties;

public class FlinkCDCPGLovniaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 指定状态后端，开启Checkpoint,每隔5秒做一个CK
       /* env.setStateBackend(new FsStateBackend(""));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);*/

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        Properties properties = new Properties();
        properties.setProperty("snapshot.mode", "never");
        SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
                .hostname("172.31.22.66")
                .port(5432)
                .database("lovina") // monitor postgres database
                .schemaList("public")  // monitor inventory schema
                .tableList("public.t_task") // monitor products table
                .username("data_stream")
                .password("AbUYpb5q5cyK9QPaEhe0")
                .decodingPluginName("wal2json") // pg解码插件
                .slotName("flink_lovina_slot") // 复制槽名称 不能重复
                .debeziumProperties(properties)
                .deserializer(new MyStringDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        dataStreamSource.print();
        dataStreamSource.addSink(MyKafkaUtil.getKafkaProducer("db-change-log-lovina"));
        env.execute();
    }
}
