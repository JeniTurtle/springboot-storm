package com.jouryu.storm.bolt;

import com.jouryu.config.ApplicationConfiguration;
import com.jouryu.constants.StormContants;
import com.jouryu.hbase.HbaseService;
import com.jouryu.netty.SocketClient;
import com.jouryu.utils.SpringUtil;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

/**
 * Created by tomorrow on 18/11/22.
 */
public class WaveletHandlerBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(WaveletHandlerBolt.class);

    private OutputCollector collector;

    private String monitorCode;

    private ApplicationConfiguration appConfig;

    public WaveletHandlerBolt(String monitorCode) {
        this.monitorCode = monitorCode;
    }

    @Override
    public void prepare(Map map, TopologyContext arg1, OutputCollector collector) {
        this.collector = collector;
        appConfig = SpringUtil.getBean(ApplicationConfiguration.class);
    }

    @Override
    public void execute(Tuple tuple) {
        String sensorType = tuple.getStringByField(StormContants.KAFKA_SENSOR_TYPE_FIELD);
        JSONArray waveletJsonData = (JSONArray) tuple.getValueByField(StormContants.WAVELET_JSON_DATA_FIELD);
        JSONObject regressionJsonData = (JSONObject) tuple.getValueByField(StormContants.REGRESSION_JSON_DATA_FIELD);
        String firstDispatchTime = (String) tuple.getValueByField(StormContants.FIRST_DISPATCH_TIME_FIELD);

        // 按照key排序得到一个有序map
        LinkedHashMap<String, Double> sortedMap = sortJsonObject(regressionJsonData);
        //把计算结果存储到数据库
        dataStorage(sortedMap, firstDispatchTime, sensorType);
        // 把计算结果发送给socket服务
        sendMessage2SocketServer(sensorType, waveletJsonData, regressionJsonData);
    }

    /**
     * cleanup是IBolt接口中定义,用于释放bolt占用的资源。
     * Storm在终止一个bolt之前会调用这个方法。
     */
    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    /**
     * 往socket server发送计算结果, socket server会返回给所有web客户端
     * @param sensorType
     * @param waveletJsonData
     * @param regressionJsonData
     */
    private void sendMessage2SocketServer(String sensorType, JSONArray waveletJsonData, JSONObject regressionJsonData) {
        if (SocketClient.channel != null) {
            JSONObject messageObject = new JSONObject();
            messageObject.put("msgType", "waveletAnalysis");
            messageObject.put("monitorCode", monitorCode);
            messageObject.put("sensorType", sensorType);
            messageObject.put("regressionData", regressionJsonData);
            messageObject.put("waveletData", waveletJsonData);

            String message = messageObject.toJSONString();
            TextWebSocketFrame frame = new TextWebSocketFrame(message);
            SocketClient.channel.writeAndFlush(frame);
        }
    }

    private void dataStorage(LinkedHashMap<String, Double> regressionData, String firstDispatchTime, String sensorType) {
        HbaseService hbaseService = SpringUtil.getBean(HbaseService.class);

        // 获取有序map的第一个元素
        Entry<String, Double> firstEntry = regressionData.entrySet().iterator().next();

        String tableName = appConfig.getHbasePrefix() + monitorCode;

        // 判断是第一次计算, 那么往hbase保存所有计算结果
        if (firstEntry.getKey().equals(firstDispatchTime)) {
            batchWriteHbase(hbaseService, tableName, sensorType, regressionData);
        } else {  // 如果不是第一次计算, 那么只保存最后一条数据
            try {
                // 获取有序map的最后一个元素
                Entry<String, Double> lastEntry = getTailByReflection(regressionData);
                writeHbase(hbaseService, tableName, sensorType, lastEntry.getKey(), lastEntry.getValue());
            } catch (Exception e) {
                logger.error(e.getMessage());
                e.printStackTrace();
            }
        }
        logger.info("小波分析结果:" + regressionData);
    }

    /**
     * 往hbase写入单条数据
     * @param hbaseService
     * @param tableName
     * @param sensorCode
     * @param timestamp
     * @param value
     */
    private void writeHbase(HbaseService hbaseService, String tableName, String sensorCode, String timestamp, Double value) {
        SimpleDateFormat rowTimeFormator = new SimpleDateFormat("yyyyMMddHH");
        SimpleDateFormat colTimeFormator = new SimpleDateFormat("yyyyMMddHHmmss");
        String hourTime = rowTimeFormator.format(Long.parseLong(timestamp));
        String secondTime = colTimeFormator.format(Long.parseLong(timestamp));

        List<Mutation> datas = new ArrayList<>();
        // rowkey为当前小时, 一小时内的数据都保存在一条记录中
        Put put = new Put(Bytes.toBytes(sensorCode + hourTime));
        put.addColumn(Bytes.toBytes("analysisData"), Bytes.toBytes(secondTime), Bytes.toBytes(String.valueOf(value)));
        datas.add(put);
        List<Mutation> results = hbaseService.saveOrUpdate(tableName, datas);
        if (results.size() < 1) {
            logger.error("hbase数据库写入失败!");
        }
    }

    /**
     * 往hbase批量写入数据
     * @param hbaseService
     * @param tableName
     * @param sensorCode
     * @param data
     */
    private void batchWriteHbase(HbaseService hbaseService, String tableName, String sensorCode, Map<String, Double> data) {
        for (Entry<String, Double> entry : data.entrySet()) {
            writeHbase(hbaseService, tableName, sensorCode, entry.getKey(), entry.getValue());
        }
    }

    /**
     * JSONObject排序, 不考虑多层结构
     * @param obj
     * @return
     */
    private static LinkedHashMap<String, Double> sortJsonObject(Map<String, Double> obj) {
        LinkedHashMap map = new LinkedHashMap<>();
        List<String> list = new ArrayList<>();

        list.addAll(obj.keySet());
        Collections.sort(list);

        for (String key : list) {
            map.put(key, obj.get(key));
        }
        return map;
    }

    /**
     * 获取LinkedHashMap最后一个元素
     * @param map
     * @param <K>
     * @param <V>
     * @return
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    private static <K, V> Entry<K, V> getTailByReflection(LinkedHashMap<K, V> map)
            throws NoSuchFieldException, IllegalAccessException {
        Field tail = map.getClass().getDeclaredField("tail");
        tail.setAccessible(true);
        return (Entry<K, V>) tail.get(map);
    }
}
