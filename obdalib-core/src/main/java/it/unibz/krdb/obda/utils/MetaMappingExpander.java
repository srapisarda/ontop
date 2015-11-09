package it.unibz.krdb.obda.utils;

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

import it.unibz.krdb.obda.model.*;
import it.unibz.krdb.obda.model.impl.OBDADataFactoryImpl;
import it.unibz.krdb.obda.model.impl.OBDAVocabulary;
import it.unibz.krdb.obda.parser.SQLQueryShallowParser;
import it.unibz.krdb.sql.QualifiedAttributeID;
import it.unibz.krdb.sql.QuotedID;
import it.unibz.krdb.sql.QuotedIDFactory;
import it.unibz.krdb.sql.RelationID;
import it.unibz.krdb.sql.api.DeeplyParsedSQLQuery;
import it.unibz.krdb.sql.api.ProjectionJSQL;
import it.unibz.krdb.sql.api.ShallowlyParsedSQLQuery;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;


/**
 * 
 * @author xiao
 *
 */
public class MetaMappingExpander {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	
	private final Connection connection;
	private final QuotedIDFactory idfac;
	private final OBDADataFactory dfac = OBDADataFactoryImpl.getInstance();

    /**
	 *
	 * 
	 * @param connection a JDBC connection
	 */
	public MetaMappingExpander(Connection connection, QuotedIDFactory idfac) {
		this.connection = connection;
		this.idfac = idfac;
	}

	/**
	 * this method expand the input mappings, which may include meta mappings, to the concrete mappings
	 * 
	 * @param splittedMappings
	 * 		a list of mappings, which may include meta mappings
	 * @return
	 * 		expanded normal mappings
	 * @throws SQLException 
	 * @throws JSQLParserException 
	 */
	public Collection<OBDAMappingAxiom> expand(Collection<OBDAMappingAxiom> splittedMappings) throws SQLException, JSQLParserException {

		List<OBDAMappingAxiom> expandedMappings = new LinkedList<>();

		for (OBDAMappingAxiom mapping : splittedMappings) {

			List<Function> targetQuery = mapping.getTargetQuery();
			Function bodyAtom = targetQuery.get(0);
			Predicate pred = bodyAtom.getFunctionSymbol();

			if (!pred.isTriplePredicate()){
				// for normal mappings, we do not need to expand it.
				expandedMappings.add(mapping);	
			} 
			else {

				int arity;
				if (isURIRDFType(bodyAtom.getTerm(1))) {
					// variables are in the position of object
					arity = 1;
				} 
				else {
					// variables are in the position of predicate
					arity = 2;
				}
				
				List<Variable> varsInTemplate = getVariablesInTemplate(bodyAtom, arity);			
				if (varsInTemplate.isEmpty()) {
					throw new IllegalArgumentException("No Variables could be found for this metamapping. Check that the variable in the metamapping is enclosed in a URI, for instance http://.../{var}");
				}
				
				// Construct the SQL query tree from the source query we do not work with views 
				OBDASQLQuery sourceQuery = mapping.getSourceQuery();
				ShallowlyParsedSQLQuery sourceQueryParsed = SQLQueryShallowParser.parse(idfac, sourceQuery.toString());
				
				// distinctParamsProjection.addAll(columnsForTemplate);
				
				/**
				 * The query for params is almost the same with the original source query, except that
				 * we only need to distinct project the columns needed for the template expansion 
				 */
				
				List<SelectExpressionItem>	columnList = sourceQueryParsed.getProjection().getColumnList();
				
				List<SelectExpressionItem> columnsForTemplate = getColumnsForTemplate(varsInTemplate, columnList, idfac);
				
				List<List<String>> paramsForClassTemplate = getParamsForClassTemplate(sourceQueryParsed, columnsForTemplate, varsInTemplate, idfac);
				
				List<SelectExpressionItem>  columnsForValues = new ArrayList<>(columnList);
				columnsForValues.removeAll(columnsForTemplate);
				
				String id = mapping.getId();
				
				for(List<String> params : paramsForClassTemplate) {
					String newId = IDGenerator.getNextUniqueID(id + "#");
					OBDAMappingAxiom newMapping = instantiateMapping(newId, targetQuery,
							bodyAtom, sourceQueryParsed, columnsForTemplate,
							columnsForValues, params, arity, idfac);
										
					expandedMappings.add(newMapping);	
					
					log.debug("Expanded Mapping: {}", newMapping);
				}
				
			}

		}
		
		return expandedMappings;
	}

	private List<List<String>> getParamsForClassTemplate(ShallowlyParsedSQLQuery  sourceQueryParsed, List<SelectExpressionItem> columnsForTemplate, List<Variable> varsInTemplate, QuotedIDFactory idfac) throws SQLException {
		
		/**
		 * The query for params is almost the same with the original source query, except that
		 * we only need to distinct project the columns needed for the template expansion 
		 */

		ShallowlyParsedSQLQuery distinctParsedQuery = null;
		try {
			distinctParsedQuery = new ShallowlyParsedSQLQuery(sourceQueryParsed.getStatement(), idfac);
		}
		catch (JSQLParserException e1) {
			throw new IllegalArgumentException(e1);
			//continue;
		}

		
		ProjectionJSQL distinctParamsProjection = new ProjectionJSQL();
		distinctParamsProjection.setType(ProjectionJSQL.SELECT_DISTINCT);
		distinctParamsProjection.addAll(columnsForTemplate);

		distinctParsedQuery.setProjection(distinctParamsProjection);
		
		
		String distinctParamsSQL = distinctParsedQuery.toString();

	
		List<List<String>> paramsForClassTemplate = new LinkedList<List<String>>();
		try (Statement st = connection.createStatement()) {
			try (ResultSet rs = st.executeQuery(distinctParamsSQL)) {
				
				int varsInTemplateSize = varsInTemplate.size();
				while (rs.next()) {
					ArrayList<String> params = new ArrayList<>(varsInTemplateSize);
					for (int i = 1; i <= varsInTemplateSize; i++) {
						params.add(rs.getString(i));
					}
					paramsForClassTemplate.add(params);
				}
			}
		}
		return paramsForClassTemplate;
	}
	
	/**
	 * check if the term is {@code URI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")}
	 * @param term
	 * @return
	 */
	private static boolean isURIRDFType(Term term) {
		boolean result = true;
		if(term instanceof Function){
			Function func = (Function) term;
			if (func.getArity() != 1){
				result = false;
			} 
			else {
				result  = result && (func.getFunctionSymbol() instanceof URITemplatePredicate);
				result  = result && (func.getTerm(0) instanceof ValueConstant) &&
						((ValueConstant) func.getTerm(0)).getValue(). equals(OBDAVocabulary.RDF_TYPE);
			}
		} 
		else {
			result = false;
		}
		return result;
	}

	/**
	 * This method instantiate a meta mapping by the concrete parameters
	 *
	 * @throws JSQLParserException 
	 */
	private OBDAMappingAxiom instantiateMapping(String id, List<Function> targetQuery,
			Function bodyAtom, ShallowlyParsedSQLQuery sourceParsedQuery,
			List<SelectExpressionItem> columnsForTemplate,
			List<SelectExpressionItem> columnsForValues,
			List<String> params, int arity, QuotedIDFactory idfac) throws JSQLParserException {
		
		/*
		 * First construct new Target Query 
		 */
		Function newTargetBody = expandHigherOrderAtom(bodyAtom, params, arity);
		
		Expression selection = sourceParsedQuery.getWhereClause();
		
		int j = 0;
		for (SelectExpressionItem column : columnsForTemplate) {
			
			Expression columnRefExpression = column.getExpression();
			StringValue clsStringValue = new StringValue("'" + params.get(j) + "'");
			
			//we are considering only equivalences
			BinaryExpression condition = new EqualsTo();
			condition.setLeftExpression(columnRefExpression);
			condition.setRightExpression(clsStringValue);
			
			if (selection != null) 
				selection = new AndExpression(selection, condition);
			else
				selection = condition;
			j++;	
		}
			
		
		ProjectionJSQL newProjection = new ProjectionJSQL();
		newProjection.addAll(columnsForValues);
		
		/*
		 * new statement for the source query
		 * we create a new statement with the changed projection and selection
		 */

		DeeplyParsedSQLQuery newSourceParsedQuery = new DeeplyParsedSQLQuery(sourceParsedQuery.getStatement(), idfac);
		newSourceParsedQuery.setProjection(newProjection);
		newSourceParsedQuery.setWhereClause(selection);
		
		String newSourceQuerySQL = newSourceParsedQuery.toString();
		OBDASQLQuery newSourceQuery =  dfac.getSQLQuery(newSourceQuerySQL);

		OBDAMappingAxiom newMapping = dfac.getRDBMSMappingAxiom(id, newSourceQuery, 
										Collections.singletonList(newTargetBody));
		return newMapping;
	}

	
	/**
	 * This method get the columns which will be used for the predicate template 
	 * 
	 * @param varsInTemplate
	 * @param columnList
     * @return
	 */
	private static List<SelectExpressionItem> getColumnsForTemplate(List<Variable> varsInTemplate,
			List<SelectExpressionItem> columnList, QuotedIDFactory idfac) {
		
		List<SelectExpressionItem> columnsForTemplate = new ArrayList<>(varsInTemplate.size());

		for (Variable var : varsInTemplate) {
			boolean found = false;
			for (SelectExpressionItem selectExpression : columnList) {
				
				// ROMAN (23 Sep 2015): SelectExpressionItem is of the form Expression AS Alias
				// this code does not work for complex expressions (i.e., 3 * A)
				// String expression = column.getExpression().toString();
				if (selectExpression.getExpression() instanceof Column) {
					Column c = (Column)selectExpression.getExpression();

		        	QuotedID column1 = idfac.createAttributeID(c.getColumnName());
		        	RelationID relation = null;
		        	if (c.getTable().getName() != null)
		        		relation = idfac.createRelationID(c.getTable().getSchemaName(), c.getTable().getName());
		        	
		        	QualifiedAttributeID qa = new QualifiedAttributeID(relation, column1);
		        	
					if ((selectExpression.getAlias() == null && qa.getAttribute().getName().equals(var.getName())) ||
							(selectExpression.getAlias() != null && selectExpression.getAlias().getName().equals(var.getName()))) {
						columnsForTemplate.add(selectExpression);
						found = true;
						break;
					}
				}
					
									
			}
			if(!found){
                String format = "The placeholder '%s' in the target does not occur in the body of the mapping";

                throw new IllegalStateException(String.format(format,
                        var.getName()/*,  mapping.toString()*/));
			}
		}
		
		return columnsForTemplate;
	}

	/**
	 * 
	 * This method extracts the variables in the template from the atom 
	 * <p>
	 * Example 1: 
	 * <p>
	 * arity = 1. 
	 * Input Atom:
	 * <pre>triple(t1, 'rdf:type', URI("http://example.org/{}/{}", X, Y))</pre>
	 * 
	 * Output: [X, Y]
	 * <p>
	 * Example 2: 
	 * <p>
	 * arity = 2. 
	 * Input Atom:
	 * <pre>triple(t1,  URI("http://example.org/{}/{}", X, Y), t2)</pre>
	 * 
	 * Output: [X, Y]
	 * 
	 * @param atom
	 * @param arity 
	 * @return
	 */
	private static List<Variable> getVariablesInTemplate(Function atom, int arity) {
		
		Function uriTermForPredicate = findTemplatePredicateTerm(atom, arity);
		
		int len = uriTermForPredicate.getTerms().size();
		List<Variable> vars = new ArrayList<Variable>(len - 1);

		// TODO: check when getTerms().size() != getArity() 
		
		// index 0 is for the URI template
		for (int i = 1; i < len; i++) {
			vars.add((Variable) uriTermForPredicate.getTerm(i));
		}
		return vars;
	}

	

	
	/***
	 * This method expands the higher order atom 
	 * <pre>triple(t1, 'rdf:type', URI("http://example.org/{}", X))</pre>
	 *  to 
	 *  <pre>http://example.org/cls(t1)</pre>, if X is t1
	 * 
	 * @param atom 
	 * 			a Function of the form triple(t1, 'rdf:type', X)
	 * @param values
	 * 			the concrete name of the X 
	 * @param arity 
	 * @return
	 * 			expanded atom in form of <pre>http://example.org/cls(t1)</pre>
	 */
	private Function expandHigherOrderAtom(Function atom, List<String> values, int arity) {

		Function uriTermForPredicate = findTemplatePredicateTerm(atom, arity);
		
		String uriTemplate = ((ValueConstant) uriTermForPredicate.getTerm(0)).getValue();

		String predName = URITemplates.format(uriTemplate, values);
		
		Function result = null;
		if (arity == 1) {
			Predicate p = dfac.getClassPredicate(predName);
			result = dfac.getFunction(p, atom.getTerm(0));
		} 
		else if (arity == 2) {
			Predicate p = dfac.getObjectPropertyPredicate(predName);
			result = dfac.getFunction(p, atom.getTerm(0), atom.getTerm(2));
		}
		return result;
	}
	
	/**
	 * This method finds the term for the predicate template
	 */
	private static Function findTemplatePredicateTerm(Function atom, int arity) {
		Function uriTermForPredicate;
		
		if(arity == 1) {
			uriTermForPredicate = (Function) atom.getTerm(2);
		} 
		else if (arity == 2) {
			uriTermForPredicate = (Function) atom.getTerm(1);	
		} 
		else {
			throw new IllegalArgumentException("The parameter arity should be 1 or 2");
		}
		return uriTermForPredicate;
	}	
}
