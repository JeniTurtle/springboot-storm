package com.jouryu.storm.spout;

import com.jouryu.config.ApplicationConfiguration;
import com.jouryu.storm.spout.scheme.KafkaMessageScheme;
import com.jouryu.utils.SpringUtil;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;

/**
 * Created by tomorrow on 18/11/21.
 */

public class SensorKafkaSpout {

    private SpoutConfig spoutConf;

    public SensorKafkaSpout(String kafkaTopic) {
        ApplicationConfiguration appConfig = SpringUtil.getBean(ApplicationConfiguration.class);
        String zkHosts = appConfig.getZkHosts();
        String zkRoot = appConfig.getZkRoot();
        String groupId = appConfig.getGroupId();

        // 定义spoutConfig
        spoutConf = new SpoutConfig(new ZkHosts(zkHosts, zkRoot),
                kafkaTopic,
                zkRoot,
                groupId
        );

        spoutConf.scheme = new SchemeAsMultiScheme(new KafkaMessageScheme());
    }

    public KafkaSpout generateSpout() {
        return new KafkaSpout(spoutConf);
    }
}
