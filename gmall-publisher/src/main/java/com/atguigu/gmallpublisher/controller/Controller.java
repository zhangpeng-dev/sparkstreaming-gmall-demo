package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * @ClassName Controller
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/4 19:17
 * @Version 1.0
 **/
//标注为Controller层并且标注为普通对象
@RestController
public class Controller {
    @Autowired
    private PublisherService publisherService;

    /*@Description //TODO
     * @Param [date]
     * @return java.lang.String
     **/
    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam String date) {
        //从服务端获取日活总数数据
        int dauTotal = publisherService.getDauTotal(date);

        Double amountTotal = publisherService.getOrderAmountTotal(date);

        //创建list存放最终数据
        ArrayList<Map> result = new ArrayList<>();

        //创建存放新增日活的map集合
        HashMap<String, Object> dauMap = new HashMap<>();

        //创建存放新增设备的map集合
        HashMap<String, Object> devMap = new HashMap<>();

        HashMap<String, Object> gmvMap = new HashMap<>();

        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", amountTotal);

        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        return JSON.toJSONString(result);
    }

    /*@Description //TODO
     * @Param [id, date]
     * @return java.lang.String
     **/
    @RequestMapping("realtime-hours")
    public String realtimeHours(@RequestParam String id, @RequestParam String date) {
        //获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        Map todayHourMap = null;
        Map yesterdayHourMap = null;
        if ("dau".equals(id)) {
            //从service端获取今天的日活分时数据
            todayHourMap = publisherService.getDauHours(date);

            //获取昨天的日活分时数据
            yesterdayHourMap = publisherService.getDauHours(yesterday);
        } else if ("order_amount".equals(id)) {
            todayHourMap = publisherService.getOrderAmountHourMap(date);
            yesterdayHourMap = publisherService.getOrderAmountHourMap(yesterday);
        }

        //创建结果map存储数据
        HashMap<String, Object> result = new HashMap<>();

        result.put("yesterday", yesterdayHourMap);
        result.put("today", todayHourMap);

        return JSON.toJSONString(result);
    }

    @RequestMapping("sale_detail")
    public String saleDetail(@RequestParam("date") String date,@RequestParam("startpage") Integer startPage,
                             @RequestParam("size") Integer size,@RequestParam("keyword") String keyword) throws IOException {

        return publisherService.getSaleDetail(date,startPage,size,keyword);
    }
}
