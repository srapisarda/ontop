package org.semanticweb.ontop.mongo;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BSON;
import org.junit.Before;
import org.junit.Test;
import org.semanticweb.ontop.model.CQIE;
import org.semanticweb.ontop.model.Constant;
import org.semanticweb.ontop.model.DatalogProgram;
import org.semanticweb.ontop.model.Function;
import org.semanticweb.ontop.model.OBDADataFactory;
import org.semanticweb.ontop.model.OBDAException;
import org.semanticweb.ontop.model.OBDAMappingAxiom;
import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.model.Term;
import org.semanticweb.ontop.model.Variable;
import org.semanticweb.ontop.model.Predicate.COL_TYPE;
import org.semanticweb.ontop.model.impl.OBDADataFactoryImpl;
import org.semanticweb.ontop.model.impl.OBDAVocabulary;
import org.semanticweb.ontop.sql.DBMetadata;
import org.semanticweb.ontop.sql.DataDefinition;
import org.semanticweb.ontop.sql.TableDefinition;
import org.semanticweb.ontop.sql.api.Attribute;

public class MongoGeneratorTest {
/*
	final String mongofile = "/simpleMapping.json";
	MongoQueryGenerator queryGenerator;

	private static OBDADataFactory dataFactory = OBDADataFactoryImpl.getInstance();

	private static final String ANS2 = "ans2";
	private static final String ANS3 = "ans3";
	private static final String ANS4 = "ans4";
	private static final String ANS5 = "ans5";
	private static final String ANS6 = "ans6";

	private static Predicate predStudent;
	private static Predicate predBachelorStudent;
	private static Predicate predMasterStudent;
	private static Predicate predDoctoralStudent;
	
	private static Predicate predFirstName;
	private static Predicate predLastName;
	private static Predicate predAge;
	private static Predicate predGrade;
	private static Predicate predEnrollmentDate;
	
	private static Predicate predHasCourse;
	private static Predicate predHasElementaryCourse;
	private static Predicate predHasAdvancedCourse;
	
	static {
		predStudent = dataFactory.getPredicate("http://example.org/Student", 1, new COL_TYPE[] { COL_TYPE.OBJECT });
		predBachelorStudent = dataFactory.getPredicate("http://example.org/BachelorStudent", 1, new COL_TYPE[] { COL_TYPE.OBJECT });
		predMasterStudent = dataFactory.getPredicate("http://example.org/MasterStudent", 1, new COL_TYPE[] { COL_TYPE.OBJECT });
		predDoctoralStudent = dataFactory.getPredicate("http://example.org/DoctoralStudent", 1, new COL_TYPE[] { COL_TYPE.OBJECT });
		
		predFirstName = dataFactory.getPredicate("http://example.org/firstName", 2, new COL_TYPE[] { COL_TYPE.OBJECT, COL_TYPE.STRING });
		predLastName = dataFactory.getPredicate("http://example.org/lastName", 2, new COL_TYPE[] { COL_TYPE.OBJECT, COL_TYPE.STRING });
		predAge = dataFactory.getPredicate("http://example.org/age", 2, new COL_TYPE[] { COL_TYPE.OBJECT, COL_TYPE.INTEGER });
		predGrade = dataFactory.getPredicate("http://example.org/grade", 2, new COL_TYPE[] { COL_TYPE.OBJECT, COL_TYPE.DECIMAL });
		predEnrollmentDate = dataFactory.getPredicate("http://example.org/enrollmentDate", 2, new COL_TYPE[] { COL_TYPE.OBJECT, COL_TYPE.DATETIME });
		
		predHasCourse = dataFactory.getPredicate("http://example.org/hasCourse", 2, new COL_TYPE[] { COL_TYPE.OBJECT, COL_TYPE.OBJECT });
		predHasElementaryCourse = dataFactory.getPredicate("http://example.org/hasElementaryCourse", 2, new COL_TYPE[] { COL_TYPE.OBJECT, COL_TYPE.OBJECT });
		predHasAdvancedCourse = dataFactory.getPredicate("http://example.org/hasAdvancedCourse", 2, new COL_TYPE[] { COL_TYPE.OBJECT, COL_TYPE.OBJECT });
	}
	
	private static Variable x;
	private static Variable y;
	private static Variable z;
	
	private static Variable a;
	private static Variable b;
	private static Variable c;
	private static Variable d;
	private static Variable e;
	private static Variable f;
	
	static {
		x = dataFactory.getVariable("x");
		y = dataFactory.getVariable("y");
		z = dataFactory.getVariable("z");
		
		a = dataFactory.getVariable("a");
		b = dataFactory.getVariable("b");
		c = dataFactory.getVariable("c");
		d = dataFactory.getVariable("d");
		e = dataFactory.getVariable("e");
		f = dataFactory.getVariable("f");
	}
	
	private static Constant c1;
	private static Constant c2;
	private static Constant c3;
	private static Constant c4;
	private static Constant c5;
	
	static {
		c1 = dataFactory.getConstantLiteral("John", COL_TYPE.STRING);
		c2 = dataFactory.getConstantLiteral("Smith", COL_TYPE.STRING);
		c3 = dataFactory.getConstantLiteral("25", COL_TYPE.INTEGER);
		c4 = dataFactory.getConstantLiteral("48.50", COL_TYPE.DECIMAL);
		c5 = dataFactory.getConstantLiteral("2012-03-20 00:00:00", COL_TYPE.DATETIME);
	}
	
	private static Function student;
	private static Function bachelorStudent;
	private static Function masterStudent;
	private static Function doctoralStudent;
	
	private static Function firstName;
	private static Function lastName;
	private static Function age;
	private static Function grade;
	private static Function enrollmentDate;
	
	private static Function hasCourse;
	private static Function hasElementaryCourse;
	private static Function hasAdvancedCourse;
	
	static {
		student = dataFactory.getFunction(predStudent, x);
		bachelorStudent = dataFactory.getFunction(predBachelorStudent, x);
		masterStudent = dataFactory.getFunction(predMasterStudent, x);
		doctoralStudent = dataFactory.getFunction(predDoctoralStudent, x);
		
		firstName = dataFactory.getFunction(predFirstName, x, a);
		lastName = dataFactory.getFunction(predLastName, x, b);
		age = dataFactory.getFunction(predAge, x, c);
		grade = dataFactory.getFunction(predGrade, x, d);
		enrollmentDate = dataFactory.getFunction(predEnrollmentDate, x, e);
		
		hasCourse = dataFactory.getFunction(predHasCourse, x, y);
		hasElementaryCourse = dataFactory.getFunction(predHasElementaryCourse, x, y);
		hasAdvancedCourse = dataFactory.getFunction(predHasAdvancedCourse, x, y);
	}

	@Before
	public void setUpMongo() throws IOException, InvalidMongoMappingException {
		//InputStream stream = MongoMappingParserTest.class.getResourceAsStream(mongofile);
		//MongoMappingParser parser = new MongoMappingParser(new InputStreamReader(stream));
		//List<OBDAMappingAxiom> mappings =  parser.parse();

        DBMetadata metadata = new DBMetadata();//(new MongoSchemaExtractor()).extractCollectionDefinition(mappings);
        Map<Integer, Attribute> attributes = new HashMap<>();
        attributes.put(1, new Attribute("xx", BSON.UNDEFINED));
        attributes.put(2, new Attribute("yy", BSON.STRING));
        attributes.put(3, new Attribute("zz", BSON.STRING));
        DataDefinition value = new TableDefinition("R", attributes);
        metadata.add(value);
        queryGenerator = new MongoQueryGenerator(metadata);
	}


	@Test
	public void test() throws OBDAException{
		//ans1(URI("http://www.semanticweb.org/johardi/ontologies/2013/3/Ontology1365668829973.owl#Person-{}",y),
		//		URI("http://www.semanticweb.org/johardi/ontologies/2013/3/Ontology1365668829973.owl#Person-{}",z))
		//	:- R(x,y,z), IS_NOT_NULL(y), IS_NOT_NULL(z)

		Term uriTemp = dataFactory.getConstantURI("http://www.semanticweb.org/johardi/ontologies/2013/3/Ontology1365668829973.owl#Person-{}");
		Function uriY = dataFactory.getUriTemplate(uriTemp, y);
		Function uriZ = dataFactory.getUriTemplate(uriTemp, z);
		
		Predicate ansPredicate = dataFactory.getPredicate(OBDAVocabulary.QUEST_QUERY, 2, null);
		Function ans1 = dataFactory.getFunction(ansPredicate, uriY, uriZ);
		Predicate predR = dataFactory.getPredicate("R", 3, new COL_TYPE[] { COL_TYPE.OBJECT, COL_TYPE.STRING, COL_TYPE.STRING });
		Function functR = dataFactory.getFunction(predR, x, y, z);
		Function condY = dataFactory.getFunctionIsNotNull(y);
		Function condZ = dataFactory.getFunctionIsNotNull(z);

		CQIE query = dataFactory.getCQIE(ans1, functR, condY, condZ);
		DatalogProgram program = dataFactory.getDatalogProgram(query);
		String sourceQuery = queryGenerator.generateSourceQuery(program, null);
	}*/
}
