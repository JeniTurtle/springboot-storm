package com.jouryu.storm;

import com.jouryu.common.CacheData;
import com.jouryu.constants.StormContants;
import com.jouryu.model.Monitor;
import com.jouryu.storm.bolt.*;
import com.jouryu.storm.spout.SensorKafkaSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Created by tomorrow on 18/11/21.
 */

@Component
public class SensorTopology {
    private final Logger logger = LoggerFactory.getLogger(SensorTopology.class);

    @Value("${kafkaTopicPrefix}")
    private String kafkaTopicPrefix;

    public void runStorm(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder = batchSetSpoutAndBolt(builder);

        Config conf = new Config();

        // 集群环境slots有8个，这里work设置为水文站的数量
        conf.setNumWorkers(CacheData.monitorList.size());

        try {
            // 有参数时，表示向集群提交作业，并把第一个参数当做topology名称
            // 没有参数时，本地提交
            if (args != null && args.length > 0) {
                logger.info("集群环境运行");
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } else {
                logger.info("本地环境运行");
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("sensorTopology", conf, builder.createTopology());
            }
        } catch (Exception e) {
            logger.error("storm启动失败!程序退出!", e);
            System.exit(1);
        }

        logger.info("storm启动成功...");
    }

    /**
     * 根据水文站数量, 批量设置spout和bolt
     * @return
     */
    private TopologyBuilder batchSetSpoutAndBolt(TopologyBuilder builder) {

        for (Monitor monitor : CacheData.monitorList) {
            String monitorCode = monitor.getMonitorCode();

            int sensorSize = monitor.getSensors().size();

            SensorKafkaSpout kafkaSpout = new SensorKafkaSpout(kafkaTopicPrefix + monitorCode);

            // 设置1个Executeor(线程)消费kafka数据，默认一个
            builder.setSpout(StormContants.KAFKA_SPOUT + monitorCode, kafkaSpout.generateSpout(), 1);

            // 给小波分析bolt分发数据
            // 设置numTask数量为传感器数量, 这样每个bolt task才能专门处理每个传感器的数据
            builder.setBolt(StormContants.WAVELET_DISPATCH_BOLT + monitorCode, new WaveletDispathBolt(), sensorSize)
                    .fieldsGrouping(StormContants.KAFKA_SPOUT + monitorCode, new Fields(StormContants.KAFKA_SENSOR_TYPE_FIELD))
                    .setNumTasks(sensorSize);

            // 给神经网络预测blot分发数据
            // 设置numTask数量为传感器数量, 这样每个bolt task才能专门处理每个传感器的数据
            builder.setBolt(StormContants.PREDICT_DISPATCH_BOLT + monitorCode, new PredictDispathBolt(), sensorSize)
                    .fieldsGrouping(StormContants.KAFKA_SPOUT + monitorCode, new Fields(StormContants.KAFKA_SENSOR_TYPE_FIELD))
                    .setNumTasks(sensorSize);

//            // 调用小波分析脚本
//            builder.setBolt(StormContants.WAVELET_ANALYSIS_BOLT + monitorCode, new WaveletAnalysisBolt(), sensorSize)
//                    .shuffleGrouping(StormContants.WAVELET_DISPATCH_BOLT + monitorCode)
//                    .setNumTasks(sensorSize);
//
//            // 处理小波分析计算结果
//            builder.setBolt(StormContants.WAVELET_HANDLER_BOLT + monitorCode, new WaveletHandlerBolt(monitor.getMonitorCode()), sensorSize)
//                    .shuffleGrouping(StormContants.WAVELET_ANALYSIS_BOLT + monitorCode)
//                    .setNumTasks(sensorSize);

            // 调用神经网络预测脚本
            builder.setBolt(StormContants.TRAIN_PREDICT_BOLT + monitorCode, new TrainPredictBolt(), sensorSize)
                    .shuffleGrouping(StormContants.PREDICT_DISPATCH_BOLT + monitorCode)
                    .setNumTasks(sensorSize);

            // 处理神经网络预测结果
            builder.setBolt(StormContants.PREDICT_HANDLER_BOLT + monitorCode, new PredictHandlerBolt(monitor.getMonitorCode()), sensorSize)
                    .shuffleGrouping(StormContants.TRAIN_PREDICT_BOLT + monitorCode)
                    .setNumTasks(sensorSize);
        }

        return builder;
    }
}
