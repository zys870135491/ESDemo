package com.zys.test;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class TestDemo {
    public static void main(String[] args) throws IOException {
        // 1. 获取客户端
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("localhost",9200,"http")
        ));

        // 2. 构建请求
        GetRequest getRequest = new GetRequest("book","1");
        // 3. 执行获得结果
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);

        System.out.println(response.getId());
        System.out.println(response.getSourceAsString());

    }
}
