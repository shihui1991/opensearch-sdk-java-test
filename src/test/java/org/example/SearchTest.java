package org.example;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.opensearch.*;
import com.aliyun.opensearch.sdk.dependencies.com.google.common.collect.Lists;
import com.aliyun.opensearch.sdk.generated.OpenSearch;
import com.aliyun.opensearch.sdk.generated.search.*;
import com.aliyun.opensearch.sdk.generated.search.general.SearchResult;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchClientException;
import com.aliyun.opensearch.search.SearchParamsBuilder;

import org.junit.Assert;
import org.junit.Test;

public class SearchTest {

    private static String appName = "datax_test_v3";
    private static String tableName = "datax_main_table";
    private static String accesskey = "abc_key";
    private static String secret = "abc_secret";
    private static String host = "http://opensearch-cn-hangzhou.aliyuncs.com";

    @Test
    public void testSearch()
    {
        //创建并构造OpenSearch对象
        OpenSearch openSearch = new OpenSearch(accesskey, secret, host);
//创建OpenSearchClient对象，并以OpenSearch对象作为构造参数
//        OpenSearchClient serviceClient = new OpenSearchClient(openSearch, new EagleEyeHttpClientCollector());
        OpenSearchClient serviceClient = new OpenSearchClient(openSearch);
//        serviceClient.setExpired(true);
        //创建SearcherClient对象，并以OpenSearchClient对象作为构造参数
        SearcherClient searcherClient = new SearcherClient(serviceClient);
//定义Config对象，用于设定config子句参数，指定应用名，分页，数据返回格式等等
        Config config = new Config(Lists.newArrayList(appName));
        config.setStart(0);
        config.setHits(10);
        List<String> fetchFields = new ArrayList<>();
        fetchFields.add("data_id");
        fetchFields.add("type_text");
        fetchFields.add("type_double");
        fetchFields.add("type_int");
        fetchFields.add("type_int_array");
        fetchFields.add("type_float_array");
        fetchFields.add("type_literal");
        fetchFields.add("type_short_test");
        fetchFields.add("type_float");
        fetchFields.add("type_double_array");
        fetchFields.add("type_literal_array");
        config.setFetchFields(fetchFields);
//        config.setCustomConfig()
//        config.addToCustomConfig("rerank_size:200");
//        config.addToCustomConfig("total_rank_size:200000");
//设置返回格式为fulljson格式
        config.setSearchFormat(SearchFormat.JSON);
// 创建参数对象
        SearchParams searchParams = new SearchParams(config);
// 指定搜索的关键词，这里要指定在哪个索引上搜索，如果不指定的话默认在使用“default”索引（索引字段名称是您在您的数据结构中的“索引字段列表”中对应字段。），若需多个索引组合查询，需要在setQuery处合并，否则若设置多个setQuery子句，则后面的子句会替换前面子句
        searchParams.setQuery("data_id:'data_id_lsh-15-03'");
//        searchParams.setUserId("12314");
////设置查询过滤条件
//        searchParams.setFilter("id>0");
////创建sort对象，并设置二维排序
//        Sort sort = new Sort();
//        searchParams.setSort(new Sort(Lists.newArrayList(new SortField("n", Order.DECREASE))));
//        searchParams.setFilter("");
//////设置id字段降序
//        sort.addToSortFields(new SortField("id", Order.DECREASE));
////若id相同则以RANK相关性算分升序
//        sort.addToSortFields(new SortField("RANK", Order.INCREASE));
////添加Sort对象参数
//        searchParams.setSort(sort);
//执行查询语句返回数据对象
//        Rank rank = new Rank();
//        rank.setSecondRankName("asdaf");
//        rank.setSecondRankType(RankType.CAVA_SCRIPT);
//        searchParams.setRank(rank);
//        searchParams.setRawQuery("abcdef");

        try{
            SearchParamsBuilder paramsBuilder = SearchParamsBuilder.create(searchParams);
//            paramsBuilder.addDistinct("", 0, 0, false, "", false, 0);
//            paramsBuilder.addFilter("");
            SearchResult searchResult = searcherClient.execute(searchParams);
//以字符串返回查询数据
            String result = searchResult.getResult();
            System.out.println(result);
            Thread.sleep(1000);
        } catch (OpenSearchException exception){
            exception.printStackTrace();
        } catch (OpenSearchClientException exception){
            exception.printStackTrace();
        }catch (InterruptedException e) {

        }finally {
            System.out.println(1);
        }
        Assert.assertTrue(true);
    }
}
