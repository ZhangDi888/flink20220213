package com.zhangdi.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhangdi.bean.TableProcess;
import com.zhangdi.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    //创建接收两个参数的构造器
    private OutputTag<JSONObject> hbaseTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(OutputTag<JSONObject> hbaseTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.hbaseTag = hbaseTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    private Connection connection;

    //在open方法初始化一下hbase链接
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

    }

    //value:{"db":"","tn":"","before":{},"after":{},"type":""}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //1.解析数据
        String data = JSON.parseObject(value).getString("after");
        //将String数据转成实体类
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //2.创建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }
        //3.保存到状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    //建表语句 : create table if not exists db.tn(id varchar primary key,tm_name varchar) xxx;
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;
        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }

            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuffer createTable = new StringBuffer("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            //判断是否为主键，如果是追加primary key

            String[] fields = sinkColumns.split(",");

            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];
                if (sinkPk.equals(field)) {
                    createTable.append(field + " varchar primary key ");
                } else {
                    createTable.append(field).append(" varchar ");
                }

                //判断是否是最后一个，最后一个字段不加逗号
                if (i < fields.length - 1) {
                    createTable.append(",");
                }
            }

            createTable.append(")").append(sinkExtend);
            System.out.println(createTable);

            //预编译sql
            preparedStatement = connection.prepareStatement(createTable.toString());
            //执行
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //1.获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            //2.过滤字段
            JSONObject data = value.getJSONObject("after");
            filterColumn(data, tableProcess.getSinkColumns());

            //3.分流
            value.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                //kafka数据写入主流
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                //Hbase数据写入侧输出流
                ctx.output(hbaseTag, value);
            }

        } else {
            System.out.println("该组合Key: " + key + "不存在！");
        }

    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);

        //        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
        //        while (iterator.hasNext()) {
        //            Map.Entry<String, Object> next = iterator.next();
        //            if (!columns.contains(next.getKey())) {
        //                iterator.remove();
        //            }
        //        }

        //如果data里面的字段不在发送的字段列表就剔除
        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));
    }


}
