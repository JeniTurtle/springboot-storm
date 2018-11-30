package com.jouryu.storm.bolt;

import com.jouryu.constants.StormContants;
import org.json.simple.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by tomorrow on 18/11/21.
 */

public class WaveletDispathBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(WaveletDispathBolt.class);

    private OutputCollector collector;

    private int queneLength = 300;

    private String firstDispatchTime;

    private LinkedHashMap<String, String> sensorValues;

    @Override
    public void prepare(Map map, TopologyContext arg1, OutputCollector collector) {
        this.collector = collector;
        sensorValues = new LinkedHashMap<>(queneLength);
    }

    @Override
    public void execute(Tuple tuple) {
        String sensorType = tuple.getStringByField(StormContants.KAFKA_SENSOR_TYPE_FIELD);
        String timestamp = tuple.getStringByField(StormContants.KAFKA_TIME_FIELD);
        String sensorValue = tuple.getStringByField(StormContants.KAFKA_VALUE_FIELD);

        // 把接收到的数据输出到log
        printReciveMessage(timestamp, sensorType, sensorValue);

        // 记录当前bolt task第一次获取到数据的时间戳
        if (sensorValues.size() == 0) {
            firstDispatchTime = timestamp;
        }

        sensorValues.put(timestamp, sensorValue);

        // 攒够300条就继续发送给下一个bolt
        if (sensorValues.size() >= queneLength) {
            // LinedHashMap虽然是有序的, 但python解析json后却是无序的, 所以python需要根据时间戳重新做一次排序
            JSONObject sensorJsonData = new JSONObject(sensorValues);
            // 删除第一个元素
            removeFirstItem(sensorValues);
            collector.emit(new Values(sensorJsonData, sensorType, firstDispatchTime));
        }
    }

    /**
     * cleanup是IBolt接口中定义,用于释放bolt占用的资源。
     * Storm在终止一个bolt之前会调用这个方法。
     */
    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                StormContants.SENSOR_JSON_DATA_FIELD,
                StormContants.KAFKA_SENSOR_TYPE_FIELD,
                StormContants.FIRST_DISPATCH_TIME_FIELD
        ));
    }

    private void printReciveMessage(String timestamp, String sensorType, String sensorValue) {
        SimpleDateFormat timeFormator = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = timeFormator.format(new Date(Long.parseLong(timestamp)));
        logger.info(time + " 传感器(" + sensorType + ") 从Kafka中接收到的数据: " + sensorValue);
    }

    /**
     * 获取LinkedHashMap第一个元素
     * @param map
     * @param <K>
     * @param <V>
     * @return
     */
    private <K, V> Map.Entry<K, V> getFirstItem(LinkedHashMap<K, V> map) {
        return map.entrySet().iterator().next();
    }

    private <K, V> void removeFirstItem(LinkedHashMap<K, V> map) {
        Map.Entry entry = getFirstItem(map);
        map.remove(entry.getKey());
    }
}
