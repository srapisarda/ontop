package org.semanticweb.ontop.owlrefplatform.core.sql;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.semanticweb.ontop.model.AlgebraOperatorPredicate;
import org.semanticweb.ontop.model.BooleanOperationPredicate;
import org.semanticweb.ontop.model.CQIE;
import org.semanticweb.ontop.model.Function;
import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.model.Term;
import org.semanticweb.ontop.model.Variable;
import org.semanticweb.ontop.owlrefplatform.core.queryevaluation.SQLDialectAdapter;
import org.semanticweb.ontop.sql.DBMetadata;
import org.semanticweb.ontop.sql.DataDefinition;
import org.semanticweb.ontop.sql.TableDefinition;
import org.semanticweb.ontop.sql.ViewDefinition;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class SQLQueryVariableIndex extends QueryVariableIndex{
	Multimap<Variable, String> columnReferences = HashMultimap.create();

	Map<Function, String> viewNames = new HashMap<Function, String>(); 
	Map<Function, String> tableNames = new HashMap<Function, String>();
	Map<Function, DataDefinition> dataDefinitions = new HashMap<Function, DataDefinition>();

	int dataTableCount = 0;

	private final SQLDialectAdapter sqladapter;
	private final Map<Predicate, String> sqlAnsViewMap;
	

	public SQLQueryVariableIndex(CQIE cq, DBMetadata metadata, QueryInfo queryInfo, SQLDialectAdapter sqlDialectAdapter) {
		super(cq, metadata);
		
		this.sqladapter = sqlDialectAdapter;
		this.sqlAnsViewMap = queryInfo.getSQLAnsViewMap();
		
		generateViews(cq.getBody(), metadata, queryInfo);
	}

	private void generateViews(List<Function> atoms, DBMetadata metadata, QueryInfo queryInfo) {
		for (Function atom : atoms) {
			/*
			 * This will be called recursively if necessary
			 */
			generateViewsIndexVariables(atom, metadata, queryInfo);
		}
	}

	/***
	 * We associate each atom to a view definition. This will be
	 * <p>
	 * "tablename" as "viewX" or
	 * <p>
	 * (some nested sql view) as "viewX"
	 * 
	 * <p>
	 * View definitions are only done for data atoms. Join/LeftJoin and
	 * boolean atoms are not associated to view definitions.
	 * 
	 * @param atom
	 * @param metadata 
	 * @param queryInfo 
	 * @return 
	 */
	private void generateViewsIndexVariables(Function atom, DBMetadata metadata, QueryInfo queryInfo) {
		
		if (atom.getFunctionSymbol() instanceof BooleanOperationPredicate) {
			return;
		} 
		
		else if (atom.getFunctionSymbol() instanceof AlgebraOperatorPredicate) {
			List<Term> terms = atom.getTerms();
			for (Term subatom : terms) {
				if (subatom instanceof Function) {
					generateViewsIndexVariables((Function) subatom, metadata, queryInfo);
				}
			}
		}

		Predicate tablePredicate = atom.getFunctionSymbol();
		String tableName = tablePredicate.getName();
		DataDefinition def = metadata.getDefinition(tableName);

		boolean isAnsPredicate = false;
		
		if (def == null) {
			/*
			 * There is no definition for this atom, its not a database
			 * predicate. We check if it is an ans predicate and it has a
			 * view:
			 */
			// tableName = "Q"+tableName+"View";
			tableName = String.format(MySQLQueryGenerator.VIEW_ANS_NAME, tableName);
			def = queryInfo.getViewDefinition(tableName);
			if (def == null) {
				return;
			} else {
				viewNames.put(atom, tableName);
			}
			isAnsPredicate = true;
		} else {

			String simpleTableViewName = String.format(MySQLQueryGenerator.VIEW_NAME, tableName, String.valueOf(dataTableCount));
			viewNames.put(atom, simpleTableViewName);
		}
		dataTableCount++;
		
		tableNames.put(atom, def.getName());

		dataDefinitions.put(atom, def);

		indexVariables(atom, isAnsPredicate);
	}

	/***
	 * Generates the view definition, i.e., "tablename viewname".
	 */
	public String getViewDefinition(Function atom) {
		DataDefinition def = dataDefinitions.get(atom);
		String viewname = viewNames.get(atom);
		viewname = sqladapter.sqlQuote(viewname);

		if (def instanceof TableDefinition) {
			return sqladapter.sqlTableName(tableNames.get(atom), viewname);
		}
		
		else if (def instanceof ViewDefinition) {
			String viewdef = ((ViewDefinition) def).getStatement();
			String formatView = String.format("(%s) %s", viewdef, viewname);
			return formatView;
		}

		// Should be an ans atom.
		Predicate pred = atom.getFunctionSymbol();
		String view = sqlAnsViewMap.get(pred);
		viewname = "Q" + pred + "View";
		viewname = sqladapter.sqlQuote(viewname);

		if (view != null) {
			String formatView = String.format("(%s) %s", view, viewname);
			return formatView;

		}

		throw new RuntimeException(
				"Impossible to get data definition for: " + atom
						+ ", type: " + def);
	}

	private void indexVariables(Function atom, boolean isAnsPredicate) {
		DataDefinition def = dataDefinitions.get(atom);
		Predicate atomName = atom.getFunctionSymbol();
		String viewName = viewNames.get(atom);
		viewName = sqladapter.sqlQuote(viewName);
		for (int index = 0; index < atom.getTerms().size(); index++) {
			Term term = atom.getTerms().get(index);

			if (term instanceof Variable) {
				/*
				 * the index of attributes of the definition starts from 1
				 */
				String columnName;

				//TODO need a proper method for checking whether something is an ans predicate
				//change as it was before
				if (isAnsPredicate) {
					// If I am here it means that it is not a database table
					// but a view from an Ans predicate
					int attPos = 3 * (index + 1);
					columnName = def.getAttributeName(attPos);
				} else {
					columnName = def.getAttributeName(index + 1);
				}

				columnName = MySQLQueryGenerator.removeAllQuotes(columnName);

				String reference = sqladapter.sqlQualifiedColumn(viewName, columnName);
				columnReferences.put((Variable) term, reference);
			}
		}
	}

	public Collection<String> getColumnReferences(Variable var) {
		return columnReferences.get(var);
	}

	@Override
	public String getColumnName(Variable var) {
		//TODO check here
		Collection<String> posList = getColumnReferences(var);
		if (posList == null || posList.size() == 0) {
			throw new RuntimeException(
					"Unbound variable found in WHERE clause: " + var);
		}
		return posList.iterator().next();
	}

	public Map<Function, String> getViewNames() {
		return viewNames;
	}

}
