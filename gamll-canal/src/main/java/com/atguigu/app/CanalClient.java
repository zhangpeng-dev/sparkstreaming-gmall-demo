package com.atguigu.app;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

/**
 * @ClassName CanalClient
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/4 21:12
 * @Version 1.0
 **/
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.获取canal连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true) {
            //2.获取连接
            canalConnector.connect();

            //3.指定要监控的数据库
            canalConnector.subscribe("gmall.*");

            //4.获取message
            Message message = canalConnector.get(100);

            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size() > 0) {
                for (CanalEntry.Entry entry : entries) {
                    //TODO 获取表名
                    String tableName = entry.getHeader().getTableName();

                    //Entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    //判断entryType是否为ROWDATA
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        //序列化数据
                        ByteString storeValue = entry.getStoreValue();

                        //反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //TODO 获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //TODO 获取具体的数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        //TODO 根据条件获取数据
                        handler(tableName, eventType, rowDatasList);
                    }
                }
            }
        }
    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //获取订单表的新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                //获取存放列的集合
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                //获取每个列
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    jsonObject.put(column.getName(), column.getValue());
                }
//                try {
//                    Thread.sleep(new Random().nextInt(5)*1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER, jsonObject.toString());
            }
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                //获取存放列的集合
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                //获取每个列
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    jsonObject.put(column.getName(), column.getValue());
                }
//                try {
//                    Thread.sleep(new Random().nextInt(5)*1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, jsonObject.toString());
            }
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) ||
                CanalEntry.EventType.UPDATE.equals(eventType))) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                //获取存放列的集合
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                //获取每个列
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    jsonObject.put(column.getName(), column.getValue());
                }

                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_USER, jsonObject.toString());
            }
        }
    }
}
