package com.zys.test;

import com.zys.esdemo.EsApplication;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Map;

@SpringBootTest(classes = EsApplication.class)
@RunWith(SpringRunner.class)
public class SearchTest {

    @Autowired
    RestHighLevelClient client;

    @Test
    /**
     * 查询全部
     *
     **/
    public void getAll() throws Exception {
        /**
         * 查询全部
         * GET /serchindex/_search
         * {
         *   "query": {
         *     "match_all": {}
         *   }
         * }
         */

        // 1.构建搜素请求
        SearchRequest request = new SearchRequest("serchindex");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // 获取部分字段数据
        searchSourceBuilder.fetchSource(new String[]{"name","price"},null);
        request.source(searchSourceBuilder);

        // 2.执行搜索
        SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
        // 获取结果
        SearchHits hits = searchResponse.getHits();
        // 总的数据
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String)sourceAsMap.get("name");
            String price =  (String)sourceAsMap.get("price");
            System.out.println("id:"+id+",score:"+score+",name:"+name+",price:"+price);
        }

    }

    @Test
    /**
     * 分页搜索
     */
    public void pageTest() throws Exception {
        /**
         * GET /serchindex/_search
         * {
         *   "query": {
         *     "match_all": {}
         *   },
         *   "from": 0,
         *   "size": 2
         * }
         */
        SearchRequest request = new SearchRequest("serchindex");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 分页构建
        int page = 1;
        int size = 2;
        int from = (page-1)*size;
        searchSourceBuilder.from(from);
        searchSourceBuilder.size(size);
        request.source(searchSourceBuilder);

        // 2.执行搜索
        SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
        // 获取结果
        SearchHits hits = searchResponse.getHits();
        // 总的数据
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String)sourceAsMap.get("name");
            String price =  (String)sourceAsMap.get("price");
            System.out.println("id:"+id+",score:"+score+",name:"+name+",price:"+price);
        }
    }

    @Test
    /**
     * 通过id搜素
     */
    public void idsTest() throws Exception {
        /**
         * GET /serchindex/_search
         * {
         *   "query": {
         *     "ids": {
         *       "values": [1,2]
         *     }
         *   }
         * }
         */
        SearchRequest request = new SearchRequest("serchindex");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.idsQuery().addIds("1","2","100"));
        request.source(searchSourceBuilder);

        // 2.执行搜索
        SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
        // 获取结果
        SearchHits hits = searchResponse.getHits();
        // 总的数据
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String)sourceAsMap.get("name");
            String price =  (String)sourceAsMap.get("price");
            System.out.println("id:"+id+",score:"+score+",name:"+name+",price:"+price);
        }
    }

    @Test
    /**
     * 通过match搜素
     */
    public void matchSearchTest() throws Exception {
       /*
       GET /serchindex/_search
        {
            "query": {
            "match": {
                "description":"java"
            }
        }
        */

        SearchRequest request = new SearchRequest("serchindex");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 通过match搜索
        searchSourceBuilder.query(QueryBuilders.matchQuery("description","java"));
        request.source(searchSourceBuilder);

        // 2.执行搜索
        SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
        // 获取结果
        SearchHits hits = searchResponse.getHits();
        // 总的数据
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String)sourceAsMap.get("name");
            String price =  (String)sourceAsMap.get("price");
            System.out.println("id:"+id+",score:"+score+",name:"+name+",price:"+price);
        }
    }

    @Test
    /**
     * 通过multi_match搜素
     */
    public void multiMatchSearchTest() throws Exception {
        /*
        GET /serchindex/_search
        {
            "query": {
                "multi_match": {
                    "query": "java",
                    "fields": ["description","name"]
                }
            }
        }
        */

        SearchRequest request = new SearchRequest("serchindex");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 通过multiMatch搜索
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("java","name","description"));
        request.source(searchSourceBuilder);

        // 2.执行搜索
        SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
        // 获取结果
        SearchHits hits = searchResponse.getHits();
        // 总的数据
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String)sourceAsMap.get("name");
            String price =  (String)sourceAsMap.get("price");
            System.out.println("id:"+id+",score:"+score+",name:"+name+",price:"+price);
        }
    }

    @Test
    /**
     * bool搜索
     */
    public void boolTest() throws Exception{
        /*GET /serchindex/_search
        {
            "query": {
            "bool": {
                "must": [
                {
                    "multi_match": {
                    "query": "java",
                            "fields": ["name","description"]
                }
                }
        ],
                "should": [
                {
                    "match": {
                    "timestamp": "1989-02-12"
                }
                }
        ]
            }
        }
        }*/
        SearchRequest request = new SearchRequest("serchindex");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder("java","name","description");
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("timestamp", "1989-02-12");

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.should(matchQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);
        request.source(searchSourceBuilder);

        // 2.执行搜索
        SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
        // 获取结果
        SearchHits hits = searchResponse.getHits();
        // 总的数据
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String)sourceAsMap.get("name");
            String price =  (String)sourceAsMap.get("price");
            System.out.println("id:"+id+",score:"+score+",name:"+name+",price:"+price);
        }
    }

    @Test
    /**
     * filter搜索
     */
    public void filterTest() throws Exception{
        /*GET /serchindex/_search
        {
            "query": {
            "bool": {
                "must": [
                {
                    "multi_match": {
                    "query": "java",
                            "fields": ["name","description"]
                }
                }
        ],
                "should": [
                {
                    "match": {
                    "timestamp": "1989-02-12"
                }
                }
        ],
                "filter": {
                    "range": {
                        "price": {
                            "gte": 80,
                                    "lte": 100
                        }
                    }
                }
            }
        }
        }*/
        SearchRequest request = new SearchRequest("serchindex");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder("java","name","description");
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("timestamp", "1989-02-12");

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.should(matchQueryBuilder);
        // filter
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(80).lte("100"));
        searchSourceBuilder.query(boolQueryBuilder);
        request.source(searchSourceBuilder);

        // 2.执行搜索
        SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
        // 获取结果
        SearchHits hits = searchResponse.getHits();
        // 总的数据
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String)sourceAsMap.get("name");
            String price =  (String)sourceAsMap.get("price");
            System.out.println("id:"+id+",score:"+score+",name:"+name+",price:"+price);
        }
    }

    @Test
    /**
     * sort搜索
     */
    public void sortTest() throws Exception{
        /*GET /serchindex/_search
        {
            "query": {
            "bool": {
                "must": [
                {
                    "multi_match": {
                    "query": "java",
                            "fields": ["name","description"]
                }
                }
        ],
                "should": [
                {
                    "match": {
                    "timestamp": "1989-02-12"
                }
                }
        ]
            }
        },
            "sort": [
            {
                "price": {
                "order": "desc"
            }
            }
    ]
        }*/
        SearchRequest request = new SearchRequest("serchindex");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder("java","name","description");
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("timestamp", "1989-02-12");

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.should(matchQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);
        // 排序
        searchSourceBuilder.sort("price", SortOrder.ASC);
        request.source(searchSourceBuilder);

        // 2.执行搜索
        SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
        // 获取结果
        SearchHits hits = searchResponse.getHits();
        // 总的数据
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String)sourceAsMap.get("name");
            String price =  (String)sourceAsMap.get("price");
            System.out.println("id:"+id+",score:"+score+",name:"+name+",price:"+price);
        }
    }


}



