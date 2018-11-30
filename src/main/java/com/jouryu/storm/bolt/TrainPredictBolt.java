package com.jouryu.storm.bolt;

import com.jouryu.constants.StormContants;
import com.jouryu.utils.SpringUtil;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.Map;

/**
 * Created by tomorrow on 18/11/22.
 *
 * Warning!!! 程序启动时, 会根据setTaskNum的数量, 生成对应的python进程。通过springboot监听器, 在java进程终止后,
 * 会kill掉python进程, 但可能会存在特殊情况导致python脚本并没有被终止, 最好在每次重启、关闭服务的时候检查一下
 */

public class TrainPredictBolt extends ShellBolt implements IRichBolt {

    public TrainPredictBolt() {
        super("python3", SpringUtil.getResourceFilePath("train_predict.py"));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                StormContants.SENSOR_JSON_DATA_FIELD,   // 原始数据
                StormContants.PREDICT_JSON_DATA_FIELD,  // 预测数据
                StormContants.KAFKA_SENSOR_TYPE_FIELD)  // 传感器类型
        );
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}