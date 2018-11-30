package com.jouryu.common;

import com.jouryu.model.Monitor;
import com.jouryu.model.Sensor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by tomorrow on 18/11/23.
 */

@Component
public final class CacheData {
    private CacheData() {}

    // 水文站和传感器对象
    public static List<Monitor> monitorList;

    // python process 列表
    public static List<Process> processList = new ArrayList<>();

    /**
     * 通过sensorCode获取对应的sensor对应
     * @param sensorCode
     * @return
     */
    public static Sensor getSensorBySensorCode(String sensorCode) {
        for (Monitor monitor: monitorList) {
            for (Sensor sensor : monitor.getSensors()) {
                if (sensor.getSensorCode().equals(sensorCode)) {
                    return sensor;
                }
            }
        }
        return null;
    }
}
