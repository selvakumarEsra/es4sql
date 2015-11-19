package org.elasticsearch.es4sql.query;

import org.elasticsearch.client.Client;
import org.elasticsearch.es4sql.domain.Query;
import org.elasticsearch.es4sql.exception.SqlParseException;

/**
 * Abstract class. used to transform Select object (Represents SQL query) to
 * SearchRequestBuilder (Represents ES query)
 */
public abstract class QueryAction {

	protected Query query;

	protected Client client;


	public QueryAction(Client client, Query query) {
		this.client = client;
		this.query = query;
	}


	/**
	 * Prepare the request, and return ES request.
	 * @return ActionRequestBuilder (ES request)
	 * @throws SqlParseException
	 */
	public abstract SqlElasticRequestBuilder explain() throws SqlParseException;

}
