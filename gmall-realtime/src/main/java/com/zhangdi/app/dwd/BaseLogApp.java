package com.zhangdi.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zhangdi.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//数据流：app/web -> Ngnix -> springBoot -> kafka(ods) ->kafka(dwd)
//程  序：mockLog -> Ngnix -> Logger.sh -> kafka(ZK) -> BaseLogApp -> kafka
public class BaseLogApp {

    public static void main(String[] args) throws Exception {

        //0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9820/gmall-flink-20220212/ck"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);

        //1.消费kafka数据
        String consumerTopic = "ods_base_log";
        String groupId = "ods_base_log";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(consumerTopic, groupId));

        OutputTag<String> dirtyOutput = new OutputTag<String>("Dirty"){};

        SingleOutputStreamOperator<JSONObject> kafkaToJsonDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            //2.将一条条记录转为json,如果是脏数据，传入侧输出流
            @Override
            public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    //如果是脏数据，传入侧输出流
                    context.output(dirtyOutput, value);
                }
            }
        });

        //3.新老用户对比，状态编程
        SingleOutputStreamOperator<JSONObject> jsonWithNewDS = kafkaToJsonDS
                .keyBy(json -> json.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    //定义一个状态
                    private ValueState<String> valueStats;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //对状态初始化
                        valueStats = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-stats", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {

                        //取出isnew,判断是否为1，如果为1，取出状态，判断是否为空，不为空，将isnew改为0
                        String is_new = value.getJSONObject("common").getString("is_new");
                        if ("1".equals(is_new)) {
                            String state = valueStats.value();
                            if (state != null) {
                                is_new = "0";
                                value.getJSONObject("common").put("is_new", is_new);
                            } else {
                                //如果这个key的状态没有存在过，就把状态填进去
                                valueStats.update("1");
                            }
                        }
                        return value;
                    }
                });

        //4.分流，将不同类型的数据分到测输出流

        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displaysTag = new OutputTag<String>("displays"){};
        //分流需要process函数
        SingleOutputStreamOperator<String> pageDs = jsonWithNewDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                String start = value.getString("start");
                if (start != null && start.length() > 0) {
                    ctx.output(startTag, value.toJSONString());
                } else {
                    out.collect(value.toJSONString());

                    //提取页面数据中的曝光数据发送到测输出流
                    JSONArray displays = value.getJSONArray("displays");
                    String page_id = value.getJSONObject("page").getString("page_id");

                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject jsonObject = displays.getJSONObject(i);
                            //在每条记录里添加pageId
                            jsonObject.put("page_id", page_id);
                            //输出到测输出流
                            ctx.output(displaysTag, jsonObject.toJSONString());
                        }
                    }

                }
            }
        });

        //5.sink到kafka
        DataStream<String> startDs = pageDs.getSideOutput(startTag);
        DataStream<String> displayDs = pageDs.getSideOutput(displaysTag);

        startDs.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        displayDs.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));
        pageDs.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));

        //6.执行任务
        env.execute("BaseLogApp");
    }
}
