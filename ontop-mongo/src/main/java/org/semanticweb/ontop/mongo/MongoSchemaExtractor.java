package org.semanticweb.ontop.mongo;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.semanticweb.ontop.model.CQIE;
import org.semanticweb.ontop.model.DataTypePredicate;
import org.semanticweb.ontop.model.Function;
import org.semanticweb.ontop.model.OBDAMappingAxiom;
import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.model.Term;
import org.semanticweb.ontop.model.URITemplatePredicate;
import org.semanticweb.ontop.model.Variable;
import org.semanticweb.ontop.model.impl.OBDAVocabulary;
import org.semanticweb.ontop.sql.TableDefinition;
import org.semanticweb.ontop.sql.api.Attribute;
import org.bson.BSON;

public class MongoSchemaExtractor {

	public static TableDefinition extractCollectionDefinition(List<OBDAMappingAxiom> mappings) {
		Map<Variable, Attribute> allVariables = new HashMap<>();
		
		String collectionName = null;
		for( OBDAMappingAxiom mapping: mappings ) {
			if (collectionName == null) {
				collectionName = ((MongoQuery)mapping.getSourceQuery()).getCollectionName();
			}
			CQIE targetQuery = (CQIE) mapping.getTargetQuery();
			putAll(allVariables, analyzeQuery(targetQuery));
		}
		
		//TODO: separate mappings from different collections
		
		Map<Integer, Attribute> attributes = new HashMap<>();
		int i=0;
		for (Map.Entry<Variable, Attribute> pair : allVariables.entrySet()) {
			attributes.put(new Integer(i), pair.getValue());
			i++;
		}

		TableDefinition tableDef = new TableDefinition(collectionName, attributes);
		return tableDef;
	}

	private static Map<Variable, Attribute> analyzeQuery(CQIE targetQuery) {
		Map<Variable, Attribute> queryVariables = new HashMap<>();
		
		List<Function> bodyAtoms = targetQuery.getBody();
		for (Function atom : bodyAtoms) {
			putAll( queryVariables, analyzeBodyAtom(atom) );
		}

		return queryVariables;
	}

	private static Map<Variable, Attribute> analyzeBodyAtom(Function atom) {
		Map<Variable, Attribute> atomVariables = new HashMap<>();

		List<Term> terms = atom.getTerms();
		
		// We assume that terms with position 0 correspond to subject. 
		// We use this position to understand whether an attribute corresponding to
		// a path of values in a mongo document, e.g., contact.email, name, code, etc, 
		// can be "considered" to be a primary key
		// 
		int termPosition = 0;
		for (Term term: terms) {
			putAll( atomVariables, analyzeTerm(term, termPosition) );
			termPosition++;
		}
		
		return atomVariables;
	}

	/**
	 * 
	 * @param term
	 * @param termPosition
	 * 			the position of the term in the enclosing atom. If it is 0, then this term is a subject 
	 * 			in a triple, and it is likely that the Attribute corresponding to this term is a primary key 
	 * @return
	 */
	private static Map<Variable, Attribute> analyzeTerm(Term term, int termPosition) {
		Map<Variable, Attribute> termVariables = new HashMap<>();

		if (term instanceof Function) {
			Function function = (Function) term;

			Predicate functionSymbol = function.getFunctionSymbol();
			if (functionSymbol instanceof URITemplatePredicate) {
				//Term uriTemplateTerm = function.getTerm(0);
				Variable variable = (Variable)function.getTerm(1);
				boolean primaryKey = termPosition == 0 ? true : false;
				termVariables.put( variable, new Attribute(variable.getName(), Types.OTHER, primaryKey, null) );
			} 
			else if (functionSymbol instanceof DataTypePredicate) {
				Variable variable = (Variable)function.getTerm(0);
				int sqlType = getBSONType(functionSymbol);
				termVariables.put( variable, new Attribute(variable.getName(), sqlType) );
			}
			else {
				//TODO : what to do here?
				// throw an Exception
			}
		}
		// term is a neither a URI, nor has a specified data type. Check if it is a Variable
		else if (term instanceof Variable) {
			Variable variable = (Variable) term;
			/*
			 * copied from #MappingDataTypeRepair.insertDataTyping
			 * 
			
            Predicate dataTypeFunctor = null;

            //check in the ontology if we have already information about the datatype

            Function normal = equivalenceMap.getNormal(atom);
                //Check if a datatype was already assigned in the ontology
            dataTypeFunctor= dataTypesMap.get(normal.getFunctionSymbol());



            // If the term has no data-type predicate then by default the
            // predicate is created following the database metadata of
            // column type.
            if(dataTypeFunctor==null || isBooleanDB2(dataTypeFunctor) ){

                dataTypeFunctor = getDataTypeFunctor(variable);
            }

			Term newTerm = dfac.getFunction( dataTypeFunctor, variable);
			atom.setTerm(1, newTerm);
			*/

			boolean primaryKey = termPosition == 0 ? true : false;
			termVariables.put( variable, new Attribute(variable.getName(), Types.OTHER, primaryKey, null) );
			
		}
		else {
			//error
		}
		
		return termVariables;
	}


	// TODO: check this
	private static void putAll(Map<Variable, Attribute> allVariables, Map<Variable, Attribute> newVariables) {
		for (Map.Entry<Variable, Attribute> pair : newVariables.entrySet()) {
			
			if ( allVariables.containsKey(pair.getKey()) && allVariables.get(pair.getKey()).getType() != pair.getValue().getType() ) {
				Attribute oldAttr = allVariables.get(pair.getKey());
				Attribute newAttr = pair.getValue();

				// the type is specialized in newVariables, so we update it in allVariables
				if (oldAttr.getType() == Types.OTHER) {
					// TODO: we prefer to keep primary key flag, or not?
					allVariables.put(pair.getKey(), 
							new Attribute(oldAttr.getName(), newAttr.getType(), oldAttr.isPrimaryKey() || newAttr.isPrimaryKey(), null));
				}
				// the type is already specialized in allVariables
				else if (newAttr.getType() == Types.OTHER) {
					// TODO: do we keep primary key flag?
					// otherwise do nothing
					allVariables.put(pair.getKey(), 
							new Attribute(oldAttr.getName(), oldAttr.getType(), oldAttr.isPrimaryKey() || newAttr.isPrimaryKey(), null));
				}
				else {
					// TODO: type conflict		
					//throw new Exception();
				}
			}
			else {
				allVariables.put(pair.getKey(), pair.getValue());
			}
		}
		
		// alternatively:
		// allVariables.putAll(newVariables);
	}


	// TODO: check here
	// does "the same" as #SQLGenerator.getVariableDataType(Term term, QueryAliasIndex idx) 
	// Create a class #MongoTypeMapper, a subclass of #TypeMapper?
	//
	private static int getBSONType(Predicate functionSymbol) {
		switch ( functionSymbol.getName() ) {
		case OBDAVocabulary.XSD_STRING_URI: 
			return BSON.STRING;
		case OBDAVocabulary.XSD_INTEGER_URI: 
			return BSON.NUMBER_LONG;
		case OBDAVocabulary.XSD_INT_URI: 
			return BSON.NUMBER_INT;
		case OBDAVocabulary.XSD_DOUBLE_URI: 
			return BSON.NUMBER;
		case OBDAVocabulary.XSD_FLOAT_URI: 
			return BSON.NUMBER;
		case OBDAVocabulary.XSD_BOOLEAN_URI: 
			return BSON.BOOLEAN;
		case OBDAVocabulary.XSD_DECIMAL_URI: 
			return BSON.NUMBER;
		case OBDAVocabulary.XSD_DATETIME_URI: 
			return BSON.DATE;
		default:
			return BSON.UNDEFINED;
		}
	}

}
