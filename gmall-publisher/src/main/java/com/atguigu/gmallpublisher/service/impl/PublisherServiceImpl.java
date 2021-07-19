package com.atguigu.gmallpublisher.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.GmallConstants;
import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName PublisherServiceImpl
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/4 18:58
 * @Version 1.0
 **/
//服务端实现类，备注为service，实现方法
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;
    @Autowired
    private OrderMapper orderMapper;
    @Autowired
    private JestClient jestClient;

    @Override
    public int getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHours(String date) {
        //从mapper层获取数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);
        //创建一个map接受结果数据
        HashMap<String, Long> result = new HashMap<>();
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }
        return result;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHourMap(String date) {
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);
        HashMap<String, Double> result = new HashMap<>();
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }

    @Override
    public String getSaleDetail(String date, Integer startPage, Integer size, String keyword) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤 匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //  性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //  年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((startPage - 1) * size);
        searchSourceBuilder.size(size);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_QUERY_INDEXNAME).addType("_doc").build();

        SearchResult searchResult = jestClient.execute(search);

        //TODO 获取总人数
        Long total = searchResult.getTotal();

        //TODO 获取数据详情
        //存放detail详细数据
        ArrayList<Map> detail = new ArrayList<>();

        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);

        for (SearchResult.Hit<Map, Void> hit : hits) {
            detail.add(hit.source);
        }

        //TODO 获取聚合组数据
        MetricAggregation aggregations = searchResult.getAggregations();

        //TODO 年龄
        //获取年龄聚合组数据
        TermsAggregation groupbyUserAge = aggregations.getTermsAggregation("groupby_user_age");
        List<TermsAggregation.Entry> buckets = groupbyUserAge.getBuckets();
        int low20Count = 0;
        int up30Count = 0;
        for (TermsAggregation.Entry bucket : buckets) {
            if (Integer.parseInt(bucket.getKey()) < 20) {
                low20Count += bucket.getCount();
            } else if (Integer.parseInt(bucket.getKey()) >= 30) {
                up30Count += bucket.getCount();
            }
        }
        //获取3个年龄段的占比
        double low20Ratio = Math.round(low20Count * 1000D / total) / 10D;
        double up30Ratio = Math.round(up30Count * 1000D / total) / 10D;
        double up20low30Ratio = Math.round((100D - low20Ratio - up30Ratio) * 10D) / 10D;

        //将年龄段信息封装到Option
        Option low20Opt = new Option("20岁以下", low20Ratio);
        Option up20low30Opt = new Option("20岁以下到30岁", up20low30Ratio);
        Option up30Opt = new Option("30岁及30岁以上", up30Ratio);

        //将Option存放到ageList中
        ArrayList<Option> ageList = new ArrayList<>();
        ageList.add(low20Opt);
        ageList.add(up20low30Opt);
        ageList.add(up30Opt);

        //将ageList封装到Stat
        Stat ageStat = new Stat(ageList, "用户年龄占比");

        //TODO 性别
        //获取性别聚合组信息
        TermsAggregation groupby_user_gender = aggregations.getTermsAggregation("groupby_user_gender");

        int maleCount = 0;
        List<TermsAggregation.Entry> genderBuckets = groupby_user_gender.getBuckets();
        for (TermsAggregation.Entry bucket : genderBuckets) {
            if ("M".equals(bucket.getKey())) {
                //获取数据中男性的个数
                maleCount += bucket.getCount();
            }
        }

        //获取男女占比
        double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
        double femaleRatio = Math.round((100 - maleRatio) * 10D) / 10D;

        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);

        ArrayList<Option> genderList = new ArrayList<>();
        genderList.add(maleOpt);
        genderList.add(femaleOpt);

        Stat genderStat = new Stat(genderList, "用户性别占比");

        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        //创建map存放结果数据
        HashMap<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", detail);

        return JSONObject.toJSONString(result);
    }
}
