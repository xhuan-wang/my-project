package com.xh.flink.java.sink;

import com.xh.flink.java.pojo.Event;
import com.xh.flink.java.source.ClickSource;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: xhuan_wang
 * @Title: SinkToEsTest
 * @ProjectName: my-project
 * @Description:
 * @date: 2022-04-19 14:38
 */
public class SinkToEsTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("", 9200, "http"));
        httpHosts.add(new HttpHost("", 9200, "http"));

        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element){
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element);
                        return Requests.indexRequest()
                                .index("my-index")
                                .type("my-type")
                                .source(json);
                    }
                    @Override
                    public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        requestIndexer.add(createIndexRequest(s));
                    }
                }
        );

        esSinkBuilder.setBulkFlushMaxActions(1);

        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
//                    restClientBuilder.setDefaultHeaders();
//                    restClientBuilder.setPathPrefix();
//                    restClientBuilder.setHttpClientConfigCallback();
                }
        );

        stream.map(Event::toString).addSink(esSinkBuilder.build());

        env.execute();
    }
}
