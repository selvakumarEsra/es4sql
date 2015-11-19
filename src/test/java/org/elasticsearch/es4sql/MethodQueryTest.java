package org.elasticsearch.es4sql;

import org.elasticsearch.es4sql.query.SqlElasticSearchRequestBuilder;
import org.junit.Test;
import org.elasticsearch.es4sql.exception.SqlParseException;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;


public class MethodQueryTest {


	@Test
	public void queryTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) MainTestSuite.getSearchDao().explain("select address from bank where q= query('address:880 Holmes Lane') limit 3");
		System.out.println(select);
	}


	@Test
	public void matchQueryTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) MainTestSuite.getSearchDao().explain("select address from bank where address= matchQuery('880 Holmes Lane') limit 3");
		System.out.println(select);
	}


	@Test
	public void scoreQueryTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) MainTestSuite.getSearchDao().explain("select address from bank where address= score(matchQuery('Lane'),100) or address= score(matchQuery('Street'),0.5)  order by _score desc limit 3");
		System.out.println(select);
	}


	@Test
	public void wildcardQueryTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) MainTestSuite.getSearchDao().explain("select address from bank where address= wildcardQuery('l*e')  order by _score desc limit 3");
		System.out.println(select);
	}
	

	@Test
	public void matchPhraseQueryTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) MainTestSuite.getSearchDao().explain("select address from bank where address= matchPhrase('671 Bristol Street')  order by _score desc limit 3");
		System.out.println(select);
	}
}
