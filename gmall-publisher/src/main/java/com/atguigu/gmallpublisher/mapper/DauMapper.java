package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

//操作数据
//创建mapper接口在配置文件中通过mybatis用sql实现
public interface DauMapper {

    public Integer selectDauTotal(String date);

    public List<Map> selectDauTotalHourMap(String date);

}
