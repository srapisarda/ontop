package org.semanticweb.ontop.mongo;

import java.util.Map;

import org.semanticweb.ontop.model.Predicate;

public class QueryInfo {

	private boolean isDistinct;
	private boolean isOrderBy;
	Map<Predicate, String> sqlAnsViewMap;

	public QueryInfo(boolean isDistinct, boolean isOrderBy, Map<Predicate, String> sqlAnsViewMap) {
		this.isDistinct = isDistinct;
		this.isOrderBy = isOrderBy;
		this.sqlAnsViewMap = sqlAnsViewMap;
	}

	public boolean isDistinct() {
		return isDistinct;
	}

	public boolean isOrderBy() {
		return isOrderBy;
	}
}
