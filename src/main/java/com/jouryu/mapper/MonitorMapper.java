package com.jouryu.mapper;

import com.jouryu.model.Monitor;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by tomorrow on 18/11/9.
 */

@Mapper
@Component
public interface MonitorMapper {
    List<Monitor> getMonitors();
}
