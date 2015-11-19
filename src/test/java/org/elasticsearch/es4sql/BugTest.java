package org.elasticsearch.es4sql;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;

import org.junit.Test;
import org.elasticsearch.es4sql.exception.SqlParseException;
import org.elasticsearch.es4sql.query.SqlElasticSearchRequestBuilder;


public class BugTest {

	
	@Test
	public void bug1() throws IOException, SqlParseException, SQLFeatureNotSupportedException {

        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) MainTestSuite.getSearchDao().explain("select count(*),sum(age) from bank");
		System.out.println(select);
	}
}
