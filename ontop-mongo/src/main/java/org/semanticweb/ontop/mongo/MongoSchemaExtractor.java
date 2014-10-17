package org.semanticweb.ontop.mongo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.semanticweb.ontop.model.CQIE;
import org.semanticweb.ontop.model.OBDAMappingAxiom;
import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.model.impl.OBDAVocabulary;
import org.semanticweb.ontop.sql.DBMetadata;
import org.semanticweb.ontop.sql.TableDefinition;
import org.semanticweb.ontop.sql.api.Attribute;
import org.bson.BSON;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class MongoSchemaExtractor extends AbstractSchemaExtractor {

	
	static final Map<Predicate, Integer> predicateBSONTypes;
	static final int defaultBSONType = BSON.UNDEFINED;
	
	static {
		Map<Predicate, Integer> temp = new HashMap<>();
		temp.put(OBDAVocabulary.XSD_STRING, new Integer(BSON.STRING));
		temp.put(OBDAVocabulary.XSD_INTEGER, new Integer(BSON.NUMBER_INT));
		temp.put(OBDAVocabulary.XSD_DOUBLE, new Integer(BSON.NUMBER));
		temp.put(OBDAVocabulary.XSD_BOOLEAN, new Integer(BSON.BOOLEAN));
		temp.put(OBDAVocabulary.XSD_DECIMAL, new Integer(BSON.NUMBER));
		temp.put(OBDAVocabulary.XSD_DATETIME, new Integer(BSON.DATE));		
		temp.put(OBDAVocabulary.RDFS_LITERAL, new Integer(BSON.STRING));		
		
		predicateBSONTypes = ImmutableMap.copyOf(temp);
	}
	
	public DBMetadata extractCollectionDefinition(List<OBDAMappingAxiom> mappings) {

		/* for each collection, we gather information about the types of its "variables"
		 * so that to create a TableDefinition later
		*/
		Map<String, Map<String, Attribute>> collectionVariables = new HashMap<>();
		
		for( OBDAMappingAxiom mapping: mappings ) {
			MongoQuery sourceQuery = (MongoQuery) mapping.getSourceQuery();
			String collectionName = sourceQuery.getCollectionName();
			
			Map<String, Attribute> variables = new HashMap<>();
			if(collectionVariables.containsKey(collectionName)) {
				variables = collectionVariables.get(collectionName);
			}

			CQIE targetQuery = (CQIE) mapping.getTargetQuery();
			putAll(variables, analyzeTargetQuery(targetQuery));

			putAll(variables, analyzeSourceQuery(sourceQuery));

			collectionVariables.put(collectionName, variables);
		}
		
		
		DBMetadata metadata = new DBMetadata();
		for (Map.Entry<String, Map<String, Attribute>> collection : collectionVariables.entrySet()) {
			metadata.add( new TableDefinition(collection.getKey(), convertToTableAttributes(collection.getValue())) );
		}

		return metadata;
	}

	
	private static Map<String, Attribute> analyzeSourceQuery(MongoQuery sourceQuery) {
		Map<String, Attribute> criteriaVariables = new HashMap<>();

		JsonObject filterObject = sourceQuery.getFilterCriteria();
		for ( Entry<String, JsonElement> pair : filterObject.entrySet() ) {
			int type = getBSONType(pair.getValue());
			criteriaVariables.put(pair.getKey(), new Attribute(pair.getKey(), type));
		}
		return criteriaVariables;
	}



	private static Map<Integer, Attribute> convertToTableAttributes(Map<String, Attribute> variables) {
		Map<Integer, Attribute> attributes = new HashMap<>();
		
		int i=0;
		for (Map.Entry<String, Attribute> pair : variables.entrySet()) {
			attributes.put(new Integer(i), pair.getValue());
			i++;
		}
		return attributes;
	}


	// TODO: check here
	// does "the same" as #SQLGenerator.getVariableDataType(Term term, QueryAliasIndex idx) 
	// Create a class #MongoTypeMapper, a subclass of #TypeMapper?
	//
	// For analyzing target queries
	@Override
	protected int getNativeType(Predicate functionSymbol) {
		return predicateBSONTypes.containsKey(functionSymbol) ? 
				predicateBSONTypes.get(functionSymbol).intValue() : 
					defaultBSONType;
	}

	@Override
	protected int getDefaultNativeType() {
		return defaultBSONType;
	}
	
	// For analyzing source queries
	private static int getBSONType(JsonElement value) {
		int type = BSON.UNDEFINED;
		if ( value.isJsonPrimitive() ) {
			if ( value.getAsJsonPrimitive().isNumber() ) {
				type = BSON.NUMBER;
			} else if (value.getAsJsonPrimitive().isString()) {
				type = BSON.STRING;
			} else if (value.getAsJsonPrimitive().isBoolean()) {
				type = BSON.BOOLEAN;
			}
		}
		return type;
	}


}
