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

/**
 * @ClassName CanalClientTest
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/9 19:48
 * @Version 1.0
 **/
public class CanalClientTest {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        CanalConnector canalConnector = CanalConnectors
                .newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true) {
            canalConnector.connect();

            canalConnector.subscribe("gmall.*");

            Message message = canalConnector.get(100);

            List<CanalEntry.Entry> entries = message.getEntries();

            for (CanalEntry.Entry entry : entries) {
                String tableName = entry.getHeader().getTableName();

                CanalEntry.EntryType entryType = entry.getEntryType();

                if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {

                    ByteString storeValue = entry.getStoreValue();

                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                    CanalEntry.EventType eventType = rowChange.getEventType();

                    List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                    handler(tableName, eventType, rowDatasList);
                }
            }
        }
    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        if ("orderInfo".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    jsonObject.put(column.getName(), column.getValue());
                }
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER,jsonObject.toString());
            }
        }
    }
}
