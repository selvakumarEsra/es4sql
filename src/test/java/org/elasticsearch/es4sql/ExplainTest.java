package org.elasticsearch.es4sql;

import com.google.common.io.Files;
import org.junit.Test;
import org.elasticsearch.es4sql.exception.SqlParseException;
import org.elasticsearch.es4sql.query.SqlElasticRequestBuilder;


import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLFeatureNotSupportedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class ExplainTest {

	@Test
	public void searchSanity() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
		String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/search_explain.json"), StandardCharsets.UTF_8).replaceAll("\r","");
		String result = explain(String.format("SELECT * FROM %s WHERE firstname LIKE 'A%%' AND age > 20 GROUP BY gender", TestsConstants.TEST_INDEX));

		assertThat(result, equalTo(expectedOutput));
	}

    private String explain(String sql) throws SQLFeatureNotSupportedException, SqlParseException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException {
		SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticRequestBuilder requestBuilder = searchDao.explain(sql);
        return requestBuilder.explain();
	}
}
