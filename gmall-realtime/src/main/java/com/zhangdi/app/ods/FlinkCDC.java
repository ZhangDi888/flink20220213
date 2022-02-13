package com.zhangdi.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.zhangdi.app.function.CustomerDeserialization;
import com.zhangdi.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 开启CK并指定状态后端为FS    memory  fs  rocksdb
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9820/gmall-flink-20220212/ck"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        //2.通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-20220207-flink")
//                .tableList("gmall-210325-flink.z_user_info")   //如果不添加该参数,则消费指定数据库中所有表的数据.如果指定,指定方式为db.table
                //使用自定义序列化，能以自己想要的数据格式返回
                .deserializer(new CustomerDeserialization())
                //从最新的记录开始读取
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        String sinkTopic = "ods_base_db";

        streamSource.print();

        streamSource.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //4.启动任务
        env.execute("FlinkCDC");

    }

}
