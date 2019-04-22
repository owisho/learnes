package per.owisho.learn.es;

import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortParseElement;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class EsClient {

    private static final String _index = "megacorp";

    private static final String _type = "employee";

    private static class Employee{
        static String firstName="first_name";
        static String lastName = "last_name";
        static String age = "age";
        static String about = "about";
        static String interests = "interests";
    }

    private static Settings settings = Settings.settingsBuilder().put("cluster.name","elasticsearch_v2.4").build();

    private static TransportClient client;

    static {
        try {
            client = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        scroll();
    }

    public static void create(){
        try{
            List<String> interests = Arrays.asList("use es");
            IndexResponse response = client.prepareIndex(_index,_type,"10")
                    .setSource(jsonBuilder().startObject().field(Employee.firstName,"client").field(Employee.lastName,"java")
                            .field(Employee.age,7).field(Employee.about,"test to use java client").field(Employee.interests,interests)
                            .endObject()).get();
            System.out.println(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void query(){
        GetResponse response = client.prepareGet(_index,_type,"10").setOperationThreaded(false).get();
        System.out.println(response.getSource());
    }

    public static void delete(){
        DeleteResponse response = client.prepareDelete(_index,_type,"10").get();
        System.out.println(response);
    }

    public static void update(){
        try {
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index(_index);
            updateRequest.type(_type);
            updateRequest.id("10");
            updateRequest.doc(jsonBuilder().startObject().field(Employee.firstName,"client1.0").endObject());
            client.update(updateRequest).get();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void bulk(){
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        Map<String,Object> param1 = new HashMap<>();
        param1.put(Employee.firstName,"bulk");
        param1.put(Employee.lastName,"es");
        param1.put(Employee.age,20);
        param1.put(Employee.about,"bulk client");
        param1.put(Employee.interests,"bulk operate");
        bulkRequestBuilder.add(client.prepareIndex(_index,_type).setSource(param1,XContentType.JSON));
        Map<String,Object> param2 = new HashMap<>();
        param2.put(Employee.firstName,"bulk2");
        param2.put(Employee.lastName,"es");
        param2.put(Employee.age,20);
        param2.put(Employee.about,"bulk2 client");
        param2.put(Employee.interests,"bulk2 operate");
        bulkRequestBuilder.add(client.prepareIndex(_index,_type).setSource(param2,XContentType.JSON));
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if(bulkResponse.hasFailures()){
            System.out.println(bulkResponse.buildFailureMessage());
        }
    }

    static BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            System.out.println("before operate==============");
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            System.out.println("after operate==============");
            if(response.hasFailures()){
                System.out.println(response.buildFailureMessage());
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            System.out.println("operate has exception===============");
        }
    }).setBulkActions(1000)
            .setBulkSize(new ByteSizeValue(1,ByteSizeUnit.MB))
            .setFlushInterval(TimeValue.timeValueSeconds(5))
            .setConcurrentRequests(1)
            .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100),3))
            .build();

    public static void bulkProcessor(){
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        Map<String,Object> param1 = new HashMap<>();
        param1.put(Employee.firstName,"bulk3");
        param1.put(Employee.lastName,"es");
        param1.put(Employee.age,20);
        param1.put(Employee.about,"bulk3 client");
        param1.put(Employee.interests,"bulk3 operate");
        Map<String,Object> param2 = new HashMap<>();
        param2.put(Employee.firstName,"bulk4");
        param2.put(Employee.lastName,"es");
        param2.put(Employee.age,20);
        param2.put(Employee.about,"bulk4 client");
        param2.put(Employee.interests,"bulk4 operate");
        Map<String,Object> param3 = new HashMap<>();
        param3.put(Employee.firstName,"bulk5");
        param3.put(Employee.lastName,"es");
        param3.put(Employee.age,20);
        param3.put(Employee.about,"bulk5 client");
        param3.put(Employee.interests,"bulk5 operate");
        Map<String,Object> param4 = new HashMap<>();
        param4.put(Employee.firstName,"bulk6");
        param4.put(Employee.lastName,"es");
        param4.put(Employee.age,20);
        param4.put(Employee.about,"bulk6 client");
        param4.put(Employee.interests,"bulk6 operate");
        Map<String,Object> param5 = new HashMap<>();
        param5.put(Employee.firstName,"bulk7");
        param5.put(Employee.lastName,"es");
        param5.put(Employee.age,20);
        param5.put(Employee.about,"bulk7 client");
        param5.put(Employee.interests,"bulk7 operate");
        bulkProcessor.add(new IndexRequest(_index,_type).source(param1,XContentType.JSON));
        bulkProcessor.add(new IndexRequest(_index,_type).source(param2,XContentType.JSON));
        bulkProcessor.add(new IndexRequest(_index,_type).source(param3,XContentType.JSON));
        bulkProcessor.add(new IndexRequest(_index,_type).source(param4,XContentType.JSON));
        bulkProcessor.add(new IndexRequest(_index,_type).source(param5,XContentType.JSON));

    }

    public static void bulkProcessorDelete(){
        bulkProcessor.add(new DeleteRequest(_index,_type,"AWpDFD1vu2gXbANcOSlt"));
        bulkProcessor.add(new DeleteRequest(_index,_type,"AWpDFpeAu2gXbANcOSp_"));
        bulkProcessor.add(new DeleteRequest(_index,_type,"AWpDGKEZu2gXbANcOStv"));
        bulkProcessor.add(new DeleteRequest(_index,_type,"AWpDFpeAu2gXbANcOSqA"));
        bulkProcessor.add(new DeleteRequest(_index,_type,"AWpDGKEZu2gXbANcOStu"));
        bulkProcessor.add(new DeleteRequest(_index,_type,"AWpDGKEZu2gXbANcOStw"));
        bulkProcessor.add(new DeleteRequest(_index,_type,"AWpDFD1vu2gXbANcOSlu"));
        bulkProcessor.add(new DeleteRequest(_index,_type,"AWpDFD1vu2gXbANcOSls"));
        bulkProcessor.add(new DeleteRequest(_index,_type,"AWpDFpeAu2gXbANcOSqB"));
        bulkProcessor.close();
    }

    public static void search(){
        SearchResponse response = client.prepareSearch(_index).setTypes(_type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery(Employee.firstName,"bulk7"))
                .setPostFilter(QueryBuilders.rangeQuery(Employee.age).from(0).to(100))
                .setFrom(0).setSize(50).setExplain(true)
                .execute()
                .actionGet();
        System.out.println(response);
    }

    public static void scroll(){
        QueryBuilder qb = QueryBuilders.matchQuery(Employee.firstName,"bulk");
        SearchResponse scrollResp = client.prepareSearch(_index)
                .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setQuery(qb)
                .setSize(100).execute().actionGet();
        while(true){
            for(SearchHit hit:scrollResp.getHits().getHits()){
                System.out.println(hit.getSource());
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            if(scrollResp.getHits().getHits().length==0){
                break;
            }
        }
    }

}
