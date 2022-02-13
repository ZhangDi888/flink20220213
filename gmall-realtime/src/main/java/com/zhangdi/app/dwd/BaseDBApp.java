package com.zhangdi.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.zhangdi.app.function.CustomerDeserialization;
import com.zhangdi.app.function.TableProcessFunction;
import com.zhangdi.bean.TableProcess;
import com.zhangdi.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

public class BaseDBApp {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //配置并开启检查点
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9820/gmall-flink-20220212/ck"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);

        //2.消费kafka ods_base_db 主体数据创建流
        String consumerTopic = "ods_base_db";
        String groupId = "base_db_app_20220212";
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getKafkaConsumer(consumerTopic, groupId));

        //3.将每行数据转换为JSON对象并过滤（delete） 主流
        SingleOutputStreamOperator<JSONObject> filterDS = kafkaDs.map(JSON::parseObject).filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return !"detele".equals(value.getString("type"));
            }
        });

        //4.使用FlinkCDC消费mysql配置数据并处理成 广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall2022_realtime")
                .tableList("gmall2022_realtime.table_process")
                //把这个表的历史数据全部取出来
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();

        DataStreamSource<String> dbSourceDS = env.addSource(sourceFunction);
        //创建广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        //将配置流放进广播流中
        BroadcastStream<String> broadcastDS = dbSourceDS.broadcast(mapStateDescriptor);

        //5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastDS);

        //6.分流 处理数据 广播流数据，主流数据（根据广播流数据进行处理）
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag"){};

        //7.提取kafka流数据和HBase流数据
        SingleOutputStreamOperator<JSONObject> kafka = connectDS.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));
        DataStream<JSONObject> hbaseDS = kafka.getSideOutput(hbaseTag);

        kafka.print("kafkaDS>>>>>>>>>>>>>>>>");
        hbaseDS.print("hbaseDS>>>>>>>>>>>>>>>>");

        //8.将kafka数据写入kafka主题，将HBase数据写入Phoenix表

        //9.启动任务
        env.execute();
    }
}
