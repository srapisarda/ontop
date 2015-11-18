package it.unibz.krdb.sql.api;

/*
 * #%L
 * ontop-obdalib-core
 * %%
 * Copyright (C) 2009 - 2014 Free University of Bozen-Bolzano
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import net.sf.jsqlparser.statement.select.SelectItem;


/**
 * Store the information about the Projection of the parsed query. (between SELECT... FROM)
 */
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

public class ProjectionJSQL  {
	
	/*
	 * http://www.postgresql.org/docs/9.0/static/sql-select.html#SQL-DISTINCT
	 * 
	 * SELECT [ ALL | DISTINCT [ ON ( expression [, ...] ) ] ] * | expression [ [ AS ] output_name ] [, ...]
     * 		
	 * If DISTINCT is specified, all duplicate rows are removed from the result set (one row is kept from each group of duplicates). 
	 * ALL specifies the opposite: all rows are kept; that is the default.
	 *
	 * DISTINCT ON ( expression [, ...] ) keeps only the first row of each set of rows where the given expressions evaluate to equal. 
	 * The DISTINCT ON expressions are interpreted using the same rules as for ORDER BY (see above). Note that the "first row" of 
	 * each set is unpredictable unless ORDER BY is used to ensure that the desired row appears first. For example:
	 *
	 * SELECT DISTINCT ON (location) location, time, report
     * FROM weather_reports
     * ORDER BY location, time DESC;
     * 
	 * retrieves the most recent weather report for each location. But if we had not used ORDER BY to force descending 
	 * order of time values for each location, we'd have gotten a report from an unpredictable time for each location.
	 * 
     * The DISTINCT ON expression(s) must match the leftmost ORDER BY expression(s). The ORDER BY clause will normally 
     * contain additional expression(s) that determine the desired precedence of rows within each DISTINCT ON group.
	 * 
	 */
	
	public static final int SELECT_DEFAULT = 0;
	public static final int SELECT_DISTINCT_ON = 1;
	public static final int SELECT_DISTINCT = 2;

	private final int type;

	/**
	 * Collection of columns for this projection.
	 */
	private final List<SelectItem> selectList = new ArrayList<>();
	private final List<SelectItem> selectDistinctList = new ArrayList<>(); //for the cases with DISTINCT ON
	
	/** 
	 * A new Projection JSQL. It returns the select list or select distinct list. Recognize * sign.
	 */

	public ProjectionJSQL(int value) {
		type = value;
	}

	public String getType() {
		switch (type) {
		case SELECT_DEFAULT:
			return "select";
		case SELECT_DISTINCT_ON:
			return "select distinct on";
		case SELECT_DISTINCT:
			return "select distinct";
		}
		return "";
	}

	/**
	 * Inserts this column to the projection list.
	 * 
	 * @param column
	 *            The input column object.
	 */
	public void add(SelectItem column, boolean distinctOn) {
		if (distinctOn) {
			selectDistinctList.add(column);
		}
		else{
			selectList.add(column);
		}
	}

	/**
	 * Copies all the columns in the list and appends them to the existing list.
	 * 
	 * @param columnsForValues
	 *            The input column list.
	 */
	public void addAll(List<SelectItem> columnsForValues) {
		selectList.addAll(columnsForValues);
	}

	/**
	 * Retrieves all columns that are mentioned in the SELECT clause.
	 */
	public List<SelectItem> getColumnList() {
		return selectList;
	}
	
	

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder(getType());

		if (!selectDistinctList.isEmpty()) {
			str.append("(");
			Joiner.on(", ").appendTo(str, selectDistinctList);
			str.append(")");
		}
			
		Joiner.on(", ").appendTo(str, selectList);

		return str.append(" from").toString();
	}	
}
