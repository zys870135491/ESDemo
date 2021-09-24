package com.zys.test;

import com.zys.esdemo.EsApplication;
import com.zys.esdemo.config.EsClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * ES的文档基本增查删改以及批量操作
 */
@SpringBootTest(classes = EsApplication.class)
@RunWith(SpringRunner.class)
public class DocumentTest {

    @Autowired
    RestHighLevelClient client;

    @org.junit.Test
    public void testGet() throws Exception{
        // 2. 构建请求
        GetRequest getRequest = new GetRequest("book","1");
        // 请求时只要某些字段
        String[] includes = {"name"};
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true,includes,null);
        getRequest.fetchSourceContext(fetchSourceContext);

        // 3. 执行获得结果
        // 同步查询
        //GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);

        // 异步查询
        client.getAsync(getRequest, RequestOptions.DEFAULT, new ActionListener<GetResponse>() {
            // 响应成功
            public void onResponse(GetResponse response) {
                System.out.println(response.getId());
                System.out.println(response.getSourceAsString());
            }
            // 响应失败
            public void onFailure(Exception e) {
                e.getStackTrace();
            }
        });

        Thread.sleep(10*1000);

       /* System.out.println(response.getId());
        System.out.println(response.getSourceAsString());*/
    }

    @org.junit.Test
    public void testAdd() throws Exception {
        final IndexRequest request=new IndexRequest("test_add");
        request.id("1");
        // 添加方式1
        /*String sourceJson = "{\n" +
                "   \"name\":\"testAdd1\",\n" +
                "   \"desc\":\"新增或修改内容1\"\n" +
                "}";
        request.source(sourceJson, XContentType.JSON);*/

        // 添加方式2
       /* Map<String, Object> map = new HashMap<String, Object>();
        map.put("name","testAdd2");
        map.put("desc","新增或修改内容1");
        request.source(map,XContentType.JSON);*/

        // 添加方式3
        /*XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        {
            xContentBuilder.field("name","testAdd3");
            xContentBuilder.field("desc","desc");
        }
        request.source(xContentBuilder,XContentType.JSON);*/
        // 添加方式4
        request.source("name","test2","desc","desc");

        // 可选参数
        request.timeout(TimeValue.timeValueSeconds(2));

        //request.version(7);
        //request.versionType(VersionType.EXTERNAL);

        // 异步执行
        /*client.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            public void onResponse(IndexResponse indexResponse) {
                if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)){
                    System.out.println("这是新增操作");
                } else if (indexResponse.getResult().equals(DocWriteResponse.Result.UPDATED)){
                    System.out.println("这是修改操作");
                }
            }

            public void onFailure(Exception e) {

            }
        });*/

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)){
            System.out.println("这是新增操作");
        } else if (indexResponse.getResult().equals(DocWriteResponse.Result.UPDATED)){
            System.out.println("这是修改操作");
        }

    }


    @org.junit.Test
    public void testUpade() throws Exception{
        UpdateRequest request = new UpdateRequest("test_add","1");

        request.doc("name","update1");
        // 重试次数
        request.retryOnConflict(3);

        UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);

        if(updateResponse.getResult().equals(DocWriteResponse.Result.UPDATED)){
            System.out.println("Update:"+updateResponse.getResult());
        }else if(updateResponse.getResult().equals(DocWriteResponse.Result.CREATED)){
            System.out.println("Create:"+updateResponse.getResult());
        }else if(updateResponse.getResult().equals(DocWriteResponse.Result.DELETED)){
            System.out.println("Delete:"+updateResponse.getResult());
        }else if(updateResponse.getResult().equals(DocWriteResponse.Result.NOOP)){
            // 如果修改的内容和之前的一样，ES就不进行修改了
            System.out.println("Nppp:"+updateResponse.getResult());
        }
    }

    @org.junit.Test
    public void testDelete() throws IOException {
        DeleteRequest request = new DeleteRequest("test_add", "1");
        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
        if(deleteResponse.getResult().equals(DocWriteResponse.Result.UPDATED)){
            System.out.println("Update:"+deleteResponse.getResult());
        }else if(deleteResponse.getResult().equals(DocWriteResponse.Result.CREATED)){
            System.out.println("Create:"+deleteResponse.getResult());
        }else if(deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED)){
            System.out.println("Delete:"+deleteResponse.getResult());
        }else if(deleteResponse.getResult().equals(DocWriteResponse.Result.NOOP)){
            // 如果修改的内容和之前的一样，ES就不进行修改了
            System.out.println("Nppp:"+deleteResponse.getResult());
        }

    }

    @org.junit.Test
    // 批量操作
    public void testbulk() throws IOException {
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("test_index").id("1").source("name","bulk"));
        request.add(new IndexRequest("test_index").id("2").source("name","bulk2"));
        request.add(new UpdateRequest("test_index","2").doc("name","bulk3"));
        request.add(new DeleteRequest("test_index","1"));

        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);

        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse response = bulkItemResponse.getResponse();
            switch (bulkItemResponse.getOpType()){
                case INDEX:
                    System.out.println(response.getResult());
                    break;
                case CREATE:
                    System.out.println(response.getResult());
                    break;
                case UPDATE:
                    System.out.println(response.getResult());
                    break;
                case DELETE:
                    System.out.println(response.getResult());
                    break;

            }
        }


    }
}
