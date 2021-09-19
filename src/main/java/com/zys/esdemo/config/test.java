package com.zys.esdemo.config;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;

public class test {

    @Autowired
    EsClient esClient;

    @Autowired
    RestHighLevelClient client;

}
