package com.jouryu.storm.bolt;

import com.jouryu.constants.StormContants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by tomorrow on 18/11/21.
 */

public class PredictDispathBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(PredictDispathBolt.class);

    private OutputCollector collector;

    // 因为预测需要两个小时的数据, 每5秒一条, 就是1440条, 预测步长为100条, 那么这里就是1340
    private int queneLength = 1340;

    // 预测步长, 每新进来100条, 就把1440最前面的100条干掉, 把新的追加进来
    private int predictStep = 100;

    private LinkedHashMap<String, String> sensorValues;

    private LinkedHashMap<String, String> freshValues;

    @Override
    public void prepare(Map map, TopologyContext arg1, OutputCollector collector) {
        this.collector = collector;
        freshValues = new LinkedHashMap<>(predictStep);
        sensorValues = new LinkedHashMap<>(queneLength);
    }

    @Override
    public void execute(Tuple tuple) {
        String sensorType = tuple.getStringByField(StormContants.KAFKA_SENSOR_TYPE_FIELD);
        String timestamp = tuple.getStringByField(StormContants.KAFKA_TIME_FIELD);
        String sensorValue = tuple.getStringByField(StormContants.KAFKA_VALUE_FIELD);

        // 把接收到的数据输出到log
        printReciveMessage(timestamp, sensorType, sensorValue);

        // sensorValues没攒够的时候, 往sensorValues里塞
        if (sensorValues.size() < queneLength) {
            sensorValues.put(timestamp, sensorValue);
        } else if (freshValues.size() < predictStep) {  // sensorValues攒够了, 但是freshValues没攒够
            freshValues.put(timestamp, sensorValue);  // 往freshValues里塞
        } else {  // sensorValues和freshValues都攒够了
            // 发射数据
            boltEmit(sensorType);
            // 数据格式化
            dataRegression();
        }
    }

    /**
     * 删除sensorValues前面的数据
     * 把freshValues数据追加到sensorValues后面, 并到清空freshValues
     */
    private void dataRegression() {
        Iterator<String> iter = freshValues.keySet().iterator();
        while(iter.hasNext()){
            String key = iter.next();
            removeFirstItem(sensorValues);
            sensorValues.put(key, freshValues.get(key));
            iter.remove();
        }
    }

    /**
     * 往下个bolt发射数据
     * @param sensorType
     */
    private void boltEmit(String sensorType) {
        // LinedHashMap虽然是有序的, 但python解析json后却是无序的, 所以python需要根据时间戳重新做一次排序
        JSONObject sensorJsonData = new JSONObject(sensorValues);
        collector.emit(new Values(sensorJsonData, sensorType));
    }

    /**
     * cleanup是IBolt接口中定义,用于释放bolt占用的资源。
     * Storm在终止一个bolt之前会调用这个方法。
     */
    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(StormContants.SENSOR_JSON_DATA_FIELD, StormContants.KAFKA_SENSOR_TYPE_FIELD));
    }

    private <K, V> void removeFirstItem(LinkedHashMap<K, V> map) {
        Map.Entry entry = map.entrySet().iterator().next();
        map.remove(entry.getKey());
    }

    private void printReciveMessage(String timestamp, String sensorType, String sensorValue) {
        String time = formatTime(timestamp, "yyyy-MM-dd HH:mm:ss");
        logger.info(time + " 传感器(" + sensorType + ") 从Kafka中接收到的数据: " + sensorValue);
    }

    private String formatTime(String timestamp, String format) {
        SimpleDateFormat timeFormator = new SimpleDateFormat(format);
        return timeFormator.format(new Date(Long.parseLong(timestamp)));
    }

}
