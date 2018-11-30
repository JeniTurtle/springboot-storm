package com.jouryu;

import com.jouryu.common.CacheData;
import com.jouryu.netty.SocketClient;
import com.jouryu.service.MonitorService;
import com.jouryu.storm.SensorTopology;
import com.jouryu.utils.SpringUtil;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class Application {

	public static void main(String[] args) throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(Application.class, args);
		SpringUtil springUtil = new SpringUtil();
		springUtil.setApplicationContext(context);

		// 获取水文站和传感器列表
		MonitorService service = SpringUtil.getBean(MonitorService.class);
		CacheData.monitorList = service.getMonitors();

		// 执行storm topology
		SensorTopology app = SpringUtil.getBean(SensorTopology.class);
		app.runStorm(args);

		// 启动websocket client
		SocketClient client = SpringUtil.getBean(SocketClient.class);
		client.start();
	}

}
