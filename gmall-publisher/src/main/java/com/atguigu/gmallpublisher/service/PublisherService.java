package com.atguigu.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

//业务处理层，创建接口，在其子类中实现方法
public interface PublisherService {
    //获取日活总数数据
    public int getDauTotal(String date);

    //获取日活分时数据
    public Map getDauHours(String date);

    //交易额总数
    public Double getOrderAmountTotal(String date);

    //交易额分时数据
    public Map getOrderAmountHourMap(String date);

    //灵活需求方法
    public String getSaleDetail(String date,Integer startPage,Integer size,String keyword) throws IOException;
}
