package com.zys.test;

import com.zys.esdemo.EsApplication;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@SpringBootTest(classes = EsApplication.class)
@RunWith(SpringRunner.class)
public class IndexTest {

    @Autowired
    RestHighLevelClient client;
    /*{
  "my_index" : {
    "aliases" : { },
    "mappings" : {
      "properties" : {
        "desc" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "pictureAdress" : {
          "type" : "text",
          "index" : false
        },
        "price" : {
          "type" : "scaled_float",
          "scaling_factor" : 100.0
        },
        "releaseDate" : {
          "type" : "date",
          "format" : "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd"
        },
        "title" : {
          "type" : "text",
          "analyzer" : "ik_max_word",
          "search_analyzer" : "ik_smart"
        }
      }
    },
    "settings" : {
      "index" : {
        "creation_date" : "1632365525193",
        "number_of_shards" : "1",
        "number_of_replicas" : "1",
        "uuid" : "R0gTOChfRMi7ya_7G848uw",
        "version" : {
          "created" : "7030099"
        },
        "provided_name" : "my_index"
      }
    }
  }
}*/

    /**
     * ????????????
     * @throws Exception
     */
    @Test
    public void addIndex() throws Exception {
        CreateIndexRequest request = new CreateIndexRequest("my_index");
        // ??????????????????????????????
        request.settings(Settings.builder().put("number_of_shards",1).
                                            put("number_of_replicas",1).build());
        //???1????????? ??????mapping????????????????????????
        request.mapping(" {\n" +
                "      \"properties\" : {\n" +
                "        \"pictureAdress\" : {\n" +
                "          \"type\" : \"text\",\n" +
                "          \"index\" : false\n" +
                "        },\n" +
                "        \"price\" : {\n" +
                "          \"type\" : \"scaled_float\",\n" +
                "          \"scaling_factor\" : 100.0\n" +
                "        },\n" +
                "        \"releaseDate\" : {\n" +
                "          \"type\" : \"date\",\n" +
                "          \"format\" : \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd\"\n" +
                "        },\n" +
                "        \"title\" : {\n" +
                "          \"type\" : \"text\",\n" +
                "          \"analyzer\" : \"ik_max_word\",\n" +
                "          \"search_analyzer\" : \"ik_smart\"\n" +
                "        }\n" +
                "      }\n" +
                "    }",XContentType.JSON);

        //???2????????? ??????mapping????????????????????????
        /*XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("title");
                {
                    builder.field("type","text");
                    builder.field("analyzer","ik_max_word");
                    builder.field("search_analyzer","ik_smart");
                }
                builder.endObject();
                builder.startObject("releaseDate");
                {
                    builder.field("type","date");
                    builder.field("format","yyyy-MM-dd HH:mm:ss||yyyy-MM-dd");
                }
                builder.endObject();
                builder.startObject("price");
                {
                    builder.field("type","scaled_float");
                    builder.field("scaling_factor","100");
                }
                builder.endObject();
                builder.startObject("pictureAdress");
                {
                    builder.field("type","text");
                    builder.field("index","false");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        request.mapping(builder);*/

        //????????????
        request.alias(new Alias("itheima_index_new"));

        // ????????????
        //??????????????????
        request.setTimeout(TimeValue.timeValueMinutes(2));
        //???????????????????????????
        request.setMasterTimeout(TimeValue.timeValueMinutes(1));
        // ??????????????????
        //client.indices().create(request, RequestOptions.DEFAULT);
        client.indices().createAsync(request, RequestOptions.DEFAULT, new ActionListener<CreateIndexResponse>() {
            public void onResponse(CreateIndexResponse createIndexResponse) {
                System.out.println("!!!!!!!!??????????????????");
                System.out.println(createIndexResponse.toString());
            }

            public void onFailure(Exception e) {
                System.out.println("!!!!!!!!??????????????????");
                e.printStackTrace();
            }
        });
        Thread.sleep(10*1000);
    }

    @Test
    public void getIndex() throws InterruptedException {
        GetIndexRequest request = new GetIndexRequest("my_index");
        request.local(false);//?????????????????????????????????????????????
        request.humanReadable(true);//????????????????????????????????????
        request.includeDefaults(false);//?????????????????????????????????????????????
        client.indices().getAsync(request, RequestOptions.DEFAULT, new ActionListener<GetIndexResponse>() {
            public void onResponse(GetIndexResponse getIndexResponse) {
                System.out.println("!!!!!!!!??????????????????");
                System.out.println(getIndexResponse.toString());
            }

            public void onFailure(Exception e) {
                System.out.println("!!!!!!!!??????????????????");
                e.printStackTrace();
            }
        });
        Thread.sleep(10*1000);
    }


    /**
     * ????????????
     */
     @Test
    public void deleteIndex() throws Exception {
         DeleteIndexRequest request = new DeleteIndexRequest("my_index");
         client.indices().deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<AcknowledgedResponse>() {
             public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                 System.out.println("!!!!!!!!??????????????????");
                 System.out.println(acknowledgedResponse.toString());
             }

             public void onFailure(Exception e) {
                 System.out.println("!!!!!!!!??????????????????");
                 e.printStackTrace();
             }
         });
         Thread.sleep(10*1000);
     }

    /**
     * ????????????
     */
    @Test
    public void openIndex() throws IOException {
        OpenIndexRequest request = new OpenIndexRequest("my_index");
        OpenIndexResponse open = client.indices().open(request, RequestOptions.DEFAULT);
        System.out.println(open.isAcknowledged());
    }

    /**
     * ????????????
     */
    @Test
    public void closeIndex() throws IOException {
        CloseIndexRequest request = new CloseIndexRequest("my_index");
        AcknowledgedResponse close = client.indices().close(request, RequestOptions.DEFAULT);
        System.out.println(close.isAcknowledged());
    }
}
