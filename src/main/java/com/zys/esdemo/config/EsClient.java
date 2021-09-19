package com.zys.esdemo.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class EsClient {

    @Value("${elasticsearch.hostlist}")
    private String hostlist;

    @Bean(destroyMethod = "close")
    public RestHighLevelClient restHighLevelClient(){
        String[] split = hostlist.split(",");
        HttpHost[] httpHost = new HttpHost[split.length];
        for (int i =0; i<httpHost.length;i++){
            String item = split[i];
            httpHost[i] = new HttpHost(item.split(":")[0],Integer.parseInt(item.split(":")[1]),"http");
        }
        return  new RestHighLevelClient(RestClient.builder(httpHost));
    }

}
