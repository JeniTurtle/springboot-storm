package com.jouryu.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * Created by tomorrow on 18/11/21.
 */

@Component
@PropertySource(value= "classpath:/application.properties")
public class ApplicationConfiguration {
	@Value("${kafka.zookeeper.hosts}")
    private String zkHosts;

    @Value("${kafka.zookeeper.root}")
    private String zkRoot;

    @Value("${kafka.consumer.groupId}")
    private String groupId;

    @Value("${hbasePrefix}")
    private String hbasePrefix;

	public String getZkHosts() {
		return zkHosts;
	}

    public String getZkRoot() {
        return zkRoot;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getHbasePrefix() {
        return hbasePrefix;
    }
}