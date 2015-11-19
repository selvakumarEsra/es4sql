package org.elasticsearch.es4sql;



import com.google.common.io.ByteStreams;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;

import org.elasticsearch.plugin.nlpcn.RestSqlAction;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

@RunWith(Suite.class)
@Suite.SuiteClasses({
		QueryTest.class,
		MethodQueryTest.class,
		AggregationTest.class,
		BugTest.class,
        JoinTests.class,
		ExplainTest.class,
        SqlParserTests.class,
        ShowTest.class
})
public class MainTestSuite {

	private static TransportClient client;
	private static SearchDao searchDao;

	@BeforeClass
	public static void setUp() throws Exception {

        Settings settings = Settings.builder().build();
        client = TransportClient.builder().settings(settings).build().addTransportAddress(getTransportAddress());

		NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().get();
		String clusterName = nodeInfos.getClusterName().value();
		System.out.println(String.format("Found cluster... cluster name: %s", clusterName));

		// Load test data.
        if(client.admin().indices().prepareExists(TestsConstants.TEST_INDEX).execute().actionGet().isExists()){
            client.admin().indices().prepareDelete(TestsConstants.TEST_INDEX).get();
        }
		loadBulk("src/test/resources/accounts.json");
		loadBulk("src/test/resources/online.json");
        loadBulk("src/test/resources/phrases.json");
        loadBulk("src/test/resources/dogs.json");
        loadBulk("src/test/resources/peoples.json");
        loadBulk("src/test/resources/character_complex.json");

        prepareOdbcIndex();
        loadBulk("src/test/resources/odbc-date-formats.json");

        searchDao = new SearchDao(client);

        //refresh to make sure all the docs will return on queries
        client.admin().indices().prepareRefresh(TestsConstants.TEST_INDEX).execute().actionGet();

		System.out.println("Finished the setup process...");
	}


	@AfterClass
	public static void tearDown() {
		System.out.println("teardown process...");
	}


	/**
	 * Loads all data from the json into the test
	 * elasticsearch cluster, using TEST_INDEX
	 * @param jsonPath the json file represents the bulk
	 * @throws Exception
	 */
	public static void loadBulk(String jsonPath) throws Exception {
		System.out.println(String.format("Loading file %s into elasticsearch cluster", jsonPath));

		BulkRequestBuilder bulkBuilder = client.prepareBulk();
		byte[] buffer = ByteStreams.toByteArray(new FileInputStream(jsonPath));
		bulkBuilder.add(buffer, 0, buffer.length, TestsConstants.TEST_INDEX, null);
		BulkResponse response = bulkBuilder.get();

		if(response.hasFailures()) {
			throw new Exception(String.format("Failed during bulk load of file %s. failure message: %s", jsonPath, response.buildFailureMessage()));
		}
	}
    public static void prepareSpatialIndex(String type){
        String dataMapping = "{\n" +
                "\t\""+type+"\" :{\n" +
                "\t\t\"properties\":{\n" +
                "\t\t\t\"place\":{\n" +
                "\t\t\t\t\"type\":\"geo_shape\",\n" +
                "\t\t\t\t\"tree\": \"quadtree\",\n" +
                "\t\t\t\t\"precision\": \"10km\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"center\":{\n" +
                "\t\t\t\t\"type\":\"geo_point\",\n" +
                "\t\t\t\t\"geohash\":true,\n" +
                "\t\t\t\t\"geohash_prefix\":true,\n" +
                "\t\t\t\t\"geohash_precision\":17\n" +
                "\t\t\t},\n" +
                "\t\t\t\"description\":{\n" +
                "\t\t\t\t\"type\":\"string\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";

        client.admin().indices().preparePutMapping(TestsConstants.TEST_INDEX).setType(type).setSource(dataMapping).execute().actionGet();
    }
    public static void prepareOdbcIndex(){
        String dataMapping = "{\n" +
                "\t\"odbc\" :{\n" +
                "\t\t\"properties\":{\n" +
                "\t\t\t\"odbc_time\":{\n" +
                "\t\t\t\t\"type\":\"date\",\n" +
                "\t\t\t\t\"format\": \"{'ts' ''yyyy-MM-dd HH:mm:ss.SSS''}\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"docCount\":{\n" +
                "\t\t\t\t\"type\":\"string\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";

        client.admin().indices().preparePutMapping(TestsConstants.TEST_INDEX).setType("odbc").setSource(dataMapping).execute().actionGet();
    }

	public static SearchDao getSearchDao() {
		return searchDao;
	}

	public static TransportClient getClient() {
		return client;
	}

	private static InetSocketTransportAddress getTransportAddress() throws UnknownHostException {
		String host = System.getenv("ES_TEST_HOST");
		String port = System.getenv("ES_TEST_PORT");

		if(host == null) {
			host = "localhost";
			System.out.println("ES_TEST_HOST enviroment variable does not exist. choose default 'localhost'");
		}

		if(port == null) {
			port = "9300";
			System.out.println("ES_TEST_PORT enviroment variable does not exist. choose default '9300'");
		}

		System.out.println(String.format("Connection details: host: %s. port:%s.", host, port));
		return new InetSocketTransportAddress(InetAddress.getByName(host), Integer.parseInt(port));
	}

}
