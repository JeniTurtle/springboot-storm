package com.jouryu.storm.bolt;

import com.jouryu.constants.StormContants;
import com.jouryu.utils.SpringUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Map;

/**
 * Created by tomorrow on 18/11/22.
 */

public class PredictHandlerBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(PredictHandlerBolt.class);

    private OutputCollector collector;

    private String monitorCode;

    public PredictHandlerBolt(String monitorCode) {
        this.monitorCode = monitorCode;
    }

    @Override
    public void prepare(Map map, TopologyContext arg1, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String sensorType = tuple.getStringByField(StormContants.KAFKA_SENSOR_TYPE_FIELD);
        JSONObject predictJsonData = (JSONObject) tuple.getValueByField(StormContants.PREDICT_JSON_DATA_FIELD);
        JSONObject originalJsonData = (JSONObject) tuple.getValueByField(StormContants.SENSOR_JSON_DATA_FIELD);

        // 将预测结果写入到redis缓存中
        writeRedisCatch(sensorType, predictJsonData, originalJsonData);
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
     * 写入redis缓存
     * @param sensorType
     * @param predictJsonData
     * @param originalJsonData
     */
    private void writeRedisCatch(String sensorType, JSONObject predictJsonData, JSONObject originalJsonData) {
        StringRedisTemplate stringRedisTemplate = SpringUtil.getBean(StringRedisTemplate.class);
        JSONObject resultData = new JSONObject();
        resultData.put("predictData", predictJsonData);
        resultData.put("originalData", originalJsonData);
        String redisKey = "predict_data_" + monitorCode + "_" + sensorType;
        String redisValue = resultData.toJSONString();
        stringRedisTemplate.opsForValue().set(redisKey, redisValue);
        logger.info("神经网络预测结果: " + redisValue);
    }
}
