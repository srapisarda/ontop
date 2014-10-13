package org.semanticweb.ontop.mongo;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.QueryParserUtil;
import org.semanticweb.ontop.exception.InvalidMappingExceptionWithIndicator;
import org.semanticweb.ontop.exception.InvalidPredicateDeclarationException;
import org.semanticweb.ontop.io.ModelIOManager;
import org.semanticweb.ontop.model.CQIE;
import org.semanticweb.ontop.model.DatalogProgram;
import org.semanticweb.ontop.model.Function;
import org.semanticweb.ontop.model.OBDADataFactory;
import org.semanticweb.ontop.model.OBDAException;
import org.semanticweb.ontop.model.OBDAMappingAxiom;
import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.model.SQLOBDAModel;
import org.semanticweb.ontop.model.Variable;
import org.semanticweb.ontop.model.Predicate.COL_TYPE;
import org.semanticweb.ontop.model.impl.OBDADataFactoryImpl;
import org.semanticweb.ontop.owlrefplatform.core.QuestConstants;
import org.semanticweb.ontop.owlrefplatform.core.QuestPreferences;
import org.semanticweb.ontop.owlrefplatform.core.translator.SparqlAlgebraToDatalogTranslator;
import org.semanticweb.ontop.owlrefplatform.core.unfolding.DatalogUnfolder;
import org.semanticweb.ontop.owlrefplatform.owlapi3.QuestOWL;
import org.semanticweb.ontop.owlrefplatform.owlapi3.QuestOWLConnection;
import org.semanticweb.ontop.owlrefplatform.owlapi3.QuestOWLFactory;
import org.semanticweb.ontop.owlrefplatform.owlapi3.QuestOWLResultSet;
import org.semanticweb.ontop.owlrefplatform.owlapi3.QuestOWLStatement;
import org.semanticweb.ontop.sql.DBMetadata;
import org.semanticweb.ontop.sql.TableDefinition;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLException;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.reasoner.SimpleConfiguration;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoTest {

	//@Test
	public void connectToMongoDB() throws UnknownHostException {
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );

		DB db = mongoClient.getDB( "db" );
		
		Set<String> colls = db.getCollectionNames();

		for (String s : colls) {
		    System.out.println(s);
		}
		
		
		DBCollection coll = db.getCollection("students");
		
		System.out.println(coll.getCount());
		
/*		BasicDBObject doc = new BasicDBObject("name", "MongoDB")
        .append("type", "database")
        .append("count", 1)
        .append("info", new BasicDBObject("x", 203).append("y", 102));
		coll.insert(doc);

		DBObject myDoc = coll.findOne();
		System.out.println(myDoc);
*/
		DBCursor cursor = coll.find();
		while (cursor.hasNext()) {
			DBObject obj = cursor.next();
			System.out.println(obj);
		}
	}
	
	/*@Test
	public void generateMongoQueryTest() throws OBDAException {
		OBDADataFactory factory = OBDADataFactoryImpl.getInstance();
		
		//see #LeftJoinAwareSQLGeneratorTests for examples of constructing programs
		Function intvarx = factory.getIntegerVariable("x");
		Function ans1Function = factory.getFunction(factory.getPredicate("ans1", 1, new COL_TYPE[] {Predicate.COL_TYPE.INTEGER}), intvarx);
	
		Variable varx = factory.getVariable("x");
		Function t1 = factory.getFunction(factory.getPredicate("Student", 1, new COL_TYPE[] {Predicate.COL_TYPE.INTEGER}), varx);

		//ans1(x)  :- Student(x)
		CQIE rule1 = factory.getCQIE(ans1Function, t1);
		
		DatalogProgram program = factory.getDatalogProgram();
		program.appendRule(rule1);

		MongoQueryGenerator gen = new MongoQueryGenerator();
		List<String> signature = new ArrayList<String>();
		String mongoQuery = gen.generateSourceQuery(program, signature);	
		System.out.println(mongoQuery);
	}*/
	
	/*void test() {
		String strQuery = "SELECT * WHERE { x a Student . }";
		QueryParser queryParser = QueryParserUtil.createParser(QueryLanguage.SPARQL);
		ParsedQuery parsedQuery = queryParser.parseQuery(strQuery, null);
		
		SparqlAlgebraToDatalogTranslator translator = new SparqlAlgebraToDatalogTranslator(questInstance.getUriTemplateMatcher());
		DatalogProgram program = translator.translate(parsedQuery, signature);

	
		DatalogUnfolder unfolder = new DatalogUnfolder(program.clone(), new HashMap<Predicate, List<Integer>>());

		//Flattening !!
		program = unfolder.unfold(program, "ans1", QuestConstants.BUP, false, multiTypedFunctionSymbolIndex);

	
	}*/
	
	@Test
	public void test1() throws OBDAException, OWLException, InvalidMappingExceptionWithIndicator, IOException, InvalidPredicateDeclarationException, InvalidMongoMappingException {
		final String owlfile = "src/test/resources/student.owl";
		final String obdafile = "src/test/resources/student.obda";
		final String mongofile = "src/test/resources/simpleMapping.json";

		OBDADataFactory dataFactory = OBDADataFactoryImpl.getInstance();
		SQLOBDAModel obdaModel = dataFactory.getOBDAModel();

		QuestOWLFactory factory = new QuestOWLFactory();
		factory.setOBDAController(obdaModel);
		factory.setPreferenceHolder(new QuestPreferences());

		ModelIOManager ioManager = new ModelIOManager(obdaModel);
		ioManager.load(obdafile);
		
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		OWLOntology ontology = manager.loadOntologyFromOntologyDocument((new File(owlfile)));
		QuestOWL reasoner = (QuestOWL) factory.createReasoner(ontology, new SimpleConfiguration());

		
		QuestOWLConnection conn = reasoner.getConnection();
		QuestOWLStatement st = conn.createStatement();

		String sparqlQuery = 
				"PREFIX : <http://ex.org/> \n" +
				"SELECT DISTINCT ?x \n" +
				"WHERE { ?x a :GraduateStudent . }";

		//ans1(URI("http://ex.org/Student/{}",t1_1f1)) :- students(t1_1f1,t2_1f2,t3_1f3,t4_1f4,t5_1f5,f6_lf6), IS_NOT_NULL(t1_1f1)
		//ans1(URI("http://ex.org/Student/{}",t1_1f1)) :- students(t1_1f1,t2_1f2,t3_1f3,t4_1f4,t5_1f5,f6), EQ(f6,2), IS_NOT_NULL(t1_1f1)

		QuestOWLResultSet rs = st.executeTuple(sparqlQuery);
		int count = 0;
		while (rs.nextRow()) {
			count++;
			for (int i = 1; i <= rs.getColumnCount(); i++) {
				String varName = rs.getSignature().get(i-1);
				System.out.print(varName);
				//System.out.print("=" + rs.getOWLObject(i));
				System.out.print("=" + rs.getOWLObject(varName));
				System.out.print(" ");
			}
			System.out.println();
		}
		rs.close();
		
		assertEquals(3, count);
		
		
        InputStream stream = MongoMappingParserTest.class.getResourceAsStream(mongofile);
        MongoMappingParser parser = new MongoMappingParser(new InputStreamReader(stream));
        List<OBDAMappingAxiom> mappings =  parser.parse();
        TableDefinition collectionDefinition = MongoSchemaExtractor.extractCollectionDefinition(mappings);

        DBMetadata metadata = new DBMetadata();
        metadata.add(collectionDefinition);
		MongoQueryGenerator queryGenerator = new MongoQueryGenerator(metadata);

		
		DatalogProgram programAfterUnfolding = st.getQuestStatement().getProgramAfterUnfolding();
		List<String> signature = null;
		queryGenerator.generateSourceQuery(programAfterUnfolding, signature);
		
	}
}
