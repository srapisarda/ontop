package org.semanticweb.ontop.owlrefplatform.core.sql;

import java.util.HashMap;
import java.util.Map;

import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.owlrefplatform.core.viewmanager.ViewManager;
import org.semanticweb.ontop.sql.DBMetadata;
import org.semanticweb.ontop.sql.DataDefinition;
import org.semanticweb.ontop.sql.ViewDefinition;

public class QueryInfo {

	private boolean isDistinct;
	private boolean isOrderBy;
	Map<Predicate, String> sqlAnsViewMap;
	DBMetadata viewMetadata; 

	public QueryInfo(boolean isDistinct, boolean isOrderBy, Map<Predicate, String> sqlAnsViewMap) {
		this(isDistinct, isOrderBy, sqlAnsViewMap, new DBMetadata());
	}

	public QueryInfo(boolean isDistinct, boolean isOrderBy, Map<Predicate, String> sqlAnsViewMap, DBMetadata metadata) {
		this.isDistinct = isDistinct;
		this.isOrderBy = isOrderBy;
		this.sqlAnsViewMap = new HashMap<>(sqlAnsViewMap);
		this.viewMetadata = metadata.clone();
	}

	public static QueryInfo addAnsView(QueryInfo queryInfo, Predicate pred, String view) {
		Map<Predicate, String> newSqlAnsViewMap = new HashMap<>(queryInfo.sqlAnsViewMap);
		newSqlAnsViewMap.put(pred, view);
		return new QueryInfo(queryInfo.isDistinct, queryInfo.isOrderBy, newSqlAnsViewMap, queryInfo.viewMetadata);
	}

	public static QueryInfo addViewDefinition(QueryInfo queryInfo, ViewDefinition viewU) {

		DBMetadata metadata = queryInfo.viewMetadata.clone();
		metadata.add(viewU);
		return new QueryInfo(queryInfo.isDistinct, queryInfo.isOrderBy, queryInfo.sqlAnsViewMap, metadata);
	}

	public DataDefinition getViewDefinition(String tableName) {
		return viewMetadata.getDefinition(tableName);
	}

	public boolean isDistinct() {
		return isDistinct;
	}

	public boolean isOrderBy() {
		return isOrderBy;
	}

	Map<Predicate, String> getSQLAnsViewMap() {
		return sqlAnsViewMap;
	}


}
