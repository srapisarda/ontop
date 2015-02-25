package it.unibz.krdb.obda.quest.dag;

/*
 * #%L
 * ontop-quest-owlapi3
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


import au.com.bytecode.opencsv.CSVWriter;
import it.unibz.krdb.obda.ontology.ClassExpression;
import it.unibz.krdb.obda.ontology.DataPropertyExpression;
import it.unibz.krdb.obda.ontology.ObjectPropertyExpression;
import it.unibz.krdb.obda.ontology.Ontology;
import it.unibz.krdb.obda.owlapi3.OWLAPI3TranslatorUtility;
import it.unibz.krdb.obda.owlrefplatform.core.dagjgrapht.Equivalences;
import it.unibz.krdb.obda.owlrefplatform.core.dagjgrapht.EquivalencesDAG;
import it.unibz.krdb.obda.owlrefplatform.core.dagjgrapht.TBoxReasoner;
import it.unibz.krdb.obda.owlrefplatform.core.dagjgrapht.TBoxReasonerImpl;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import java.io.*;
import java.util.ArrayList;
import java.util.Set;

public class FindLeafTest  {

    TBoxReasoner reasoner;
	private final String ontology = "src/test/resources/test/dag/well-ontology-20141125.ttl";
    private BufferedWriter writer;
    private ArrayList<String> inputFile = new ArrayList<>();

    @Before
	public void setUp() throws Exception {

        OWLOntologyManager man = OWLManager.createOWLOntologyManager();
        OWLOntology owlonto = man.loadOntologyFromOntologyDocument(new File(ontology));
        Ontology onto = OWLAPI3TranslatorUtility.translate(owlonto);

        // generate DAG
        reasoner = new TBoxReasonerImpl(onto);
//        writer = new CSVWriter(new FileWriter("src/test/resources/test/dag/WithChildren.csv"));
        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("src/test/resources/test/dag/WithChildren.txt"), "utf-8"));
        readFile();
	}

    private void readFile() {
        try (BufferedReader br = new BufferedReader(new FileReader("src/test/resources/test/dag/WithChildrenDataProperty.txt")))
        {

            String sCurrentLine;

            while ((sCurrentLine = br.readLine()) != null) {
                inputFile.add(sCurrentLine);

            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
	public void testIfTheyHaveChildren() throws IOException {

        EquivalencesDAG<DataPropertyExpression> dataPropertyDAG = reasoner.getDataPropertyDAG();
        for (Equivalences<DataPropertyExpression> propNode : dataPropertyDAG) {

            Set<Equivalences<DataPropertyExpression>> sub = dataPropertyDAG.getDirectSub(propNode);

            if(sub.size()!=0){
//                for (DataPropertyExpression equivpropNode : propNode) {
//                    System.out.println(equivpropNode + " yes");
//                }
//            }
//            else{
                for (DataPropertyExpression equivpropNode : propNode) {

                    if(inputFile.contains(equivpropNode.toString())) {
                        System.out.println(equivpropNode + " not a leaf");

                        writer.write(equivpropNode.toString());
                        writer.newLine();
                    }

                }
            }


        }


        EquivalencesDAG<ClassExpression> classDAG = reasoner.getClassDAG();
        for (Equivalences<ClassExpression> nodes : classDAG) {

            Set<Equivalences<ClassExpression>> sub = classDAG.getDirectSub(nodes);

            if(sub.size()!=0){
//                for (ClassExpression equivNode : nodes) {
//                    System.out.println(equivNode + " yes");
//                }
//            }
//            else{
                for (ClassExpression equivNode : nodes) {

                    if(inputFile.contains(equivNode.toString())) {
                        System.out.println(equivNode + " not a leaf");
//                        writer.writeNext(new String[]{equivNode.toString()});
                        writer.write(equivNode.toString());
                        writer.newLine();
                    }
                }
            }


        }

        for (Equivalences<ObjectPropertyExpression> objNode : reasoner.getObjectPropertyDAG()) {

            Set<Equivalences<ObjectPropertyExpression>> sub = reasoner.getObjectPropertyDAG().getDirectSub(objNode);

            if(sub.size()!=0){
//                for (ObjectPropertyExpression equivobjNode : objNode) {
//                    System.out.println(equivobjNode + " yes");
//                }
//            }
//            else{
                for (ObjectPropertyExpression equivobjNode : objNode) {
                    if(inputFile.contains(equivobjNode.toString())) {
                        System.out.println(equivobjNode + " not a leaf");
                        writer.write(equivobjNode.toString());
                        writer.newLine();
                    }
                }
            }


        }

        writer.close();
    }



}
