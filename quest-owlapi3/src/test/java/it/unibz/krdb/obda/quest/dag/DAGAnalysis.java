package it.unibz.krdb.obda.quest.dag;


import it.unibz.krdb.obda.ontology.ClassExpression;
import it.unibz.krdb.obda.ontology.Description;
import it.unibz.krdb.obda.ontology.Ontology;
import it.unibz.krdb.obda.owlapi3.OWLAPI3TranslatorUtility;
import it.unibz.krdb.obda.owlrefplatform.core.dagjgrapht.EquivalencesDAG;
import it.unibz.krdb.obda.owlrefplatform.core.dagjgrapht.TBoxReasoner;
import it.unibz.krdb.obda.owlrefplatform.core.dagjgrapht.TBoxReasonerImpl;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.junit.Before;
import org.junit.Test;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Using the DAG to calculate the maximum depth of the ontology
 *  and the max numbers of sibling
 *
 */
public class DAGAnalysis  {

    Logger log = LoggerFactory.getLogger(this.getClass());

    final String owlfile = "src/test/resources/dagUtil/npd-v2-ql.owl";
    final String obdafile = "src/test/resources/dagUtil/npd-v2-ql.obda";

    SimpleDirectedGraph<Description, DefaultEdge> ontologyDAG;
    SimpleDirectedGraph<Description, DefaultEdge> reversedDAG;
    TBoxReasoner reasonerTbox;

    int max_depth = 0;
    int max_siblings = 0;
    Description finalVertex;
    List<Description> finalSiblings;
    List<String[]> vertexList = new ArrayList<String[]>();
//
//
//    @Before
//    public void setUp() throws Exception {
//        try {
//
//            OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
//            OWLOntology owlonto = manager.loadOntologyFromOntologyDocument((new File(owlfile)));
//
//            Ontology o = OWLAPI3TranslatorUtility.translate(owlonto);
//
//            reasonerTbox = new TBoxReasonerImpl(o);
//
//			/*
//			 * Transform the reasoner in a SimpleDirectedGraph to use the method provided by jgrapht API
//			 */
//            ontologyDAG = new SimpleDirectedGraph<Description, DefaultEdge>(DefaultEdge.class);
//
//            EquivalencesDAG<ClassExpression> classes = reasonerTbox.getClassDAG();
//            for ( EquivalencesDAG<ClassExpression> v : classes)
//                ontologyDAG.addVertex(v.getVertex().getRepresentative());
//
//            for (Equivalences<BasicClassDescription> s : classes)
//                for (Equivalences<BasicClassDescription> t : classes.getDirectSuper(s))
//                    ontologyDAG.addEdge(s.getRepresentative(), t.getRepresentative());
//
//            EquivalencesDAG<Property> roles = reasonerTbox.getProperties();
//            for (Equivalences<Property> v : roles)
//                ontologyDAG.addVertex(v.getRepresentative());
//
//            for (Equivalences<Property> s : roles)
//                for (Equivalences<Property> t : roles.getDirectSuper(s))
//                    ontologyDAG.addEdge(s.getRepresentative(), t.getRepresentative());
//
//        } catch (Exception exc) {
//            try {
//                tearDown();
//            } catch (Exception e2) {
//                e2.printStackTrace();
//            }
//        }
//
//    }
//
//    /**
//     * Iterate depth first through the dag, using DepthListener calculate the depth of each vertex respect to the root
//     * Write a csv file with the results
//     */
//    @Test
//    public void testDepth() throws IOException {
//
//        //reverse the graph so that the root is the greatest ancestor
//        reversedDAG = new SimpleDirectedGraph<Description, DefaultEdge>(DefaultEdge.class);
//        Graphs.addGraphReversed(reversedDAG, ontologyDAG);
//
//
//        GraphIterator<Description, DefaultEdge> orderIterator = new DepthFirstIterator<Description, DefaultEdge>(reversedDAG);
//
//        orderIterator.addTraversalListener(new DepthListener());
//        while (orderIterator.hasNext()) {
//            orderIterator.next();
//        }
//
//        log.info("Final Vertex: " + finalVertex + " depth: " + max_depth);
//
//        CSVWriter writer = new CSVWriter(new FileWriter("src/test/resources/dagUtil/DepthsOntology.csv"));
//        writer.writeNext(new String[] { "Final Vertex: " + finalVertex, " depth: " + max_depth });
//        writer.writeAll(vertexList);
//
//        writer.close();
//
//    }
//
//    /**
//     * Iterate breadth first through the dag, using SiblingsListener calculate the number of siblings
//     * Write a csv file with the results
//     */
//    @Test
//    public void testNumberSiblings() throws IOException {
//
//        //reverse the graph so that the root is the greatest ancestor
//        reversedDAG = new SimpleDirectedGraph<Description, DefaultEdge>(DefaultEdge.class);
//        Graphs.addGraphReversed(reversedDAG, ontologyDAG);
//
//
//        GraphIterator<Description, DefaultEdge> orderIterator = new BreadthFirstIterator<Description, DefaultEdge>(reversedDAG);
//        orderIterator.addTraversalListener(new SiblingsListener());
//        while (orderIterator.hasNext()) {
//            orderIterator.next();
//        }
//
//        log.info("Max siblings: " + max_siblings + " vertexes: " + finalSiblings);
//
//        CSVWriter writer = new CSVWriter(new FileWriter("src/test/resources/dagUtil/SiblingsOntology.csv"));
//        writer.writeNext(new String[] { "Max siblings: " + max_siblings, " vertexes: " + finalSiblings });
//        writer.writeAll(vertexList);
//
//        writer.close();
//
//    }
//
//    /**
//     *
//     * Listener for the depth of the DAG.
//     * Use Dijkstra Shortest Path  algorithm by jgrapht to find the depth of each vertex respect to the root
//     *
//     */
//
//    private final class DepthListener extends TraversalListenerAdapter<Description, DefaultEdge> {
//
//        private Description reference; // last node
//        private boolean newComponent;
//
//        int d = 0;
//
//        // search for the new root in the graph
//        @Override
//        public void connectedComponentStarted(ConnectedComponentTraversalEvent e) {
//            newComponent = true;
//
//            log.debug("New connected component: " + newComponent);
//        }
//
//        @Override
//        public void vertexTraversed(VertexTraversalEvent<Description> e) {
//
//            Description vertex = e.getVertex();
//
//            if (newComponent) {
//                reference = vertex;
//                newComponent = false;
//            }
//
//            d = DijkstraShortestPath.findPathBetween(reversedDAG, reference, vertex).size();
//
//            if (d > 1)
//                vertexList.add(new String[] { "Vertex: " + vertex, "Root: " + reference, "Depth: " + d });
//            if (d > max_depth) {
//                max_depth = d;
//                finalVertex = vertex;
//            }
//
//            String x = "";
//            for (int i = 0; i < d; i++)
//                x += "\t";
//            log.debug(x + "vertex: " + vertex);
//
//        }
//
//    }
//
//    /**
//     *
//     * Listener to find the siblings in the DAG
//     *
//     */
//    private final class SiblingsListener extends TraversalListenerAdapter<Description, DefaultEdge> {
//
//        int s = 0;
//
//        @Override
//        public void vertexTraversed(VertexTraversalEvent<Description> e) {
//
//            Description vertex = e.getVertex();
//            List<Description> successors = Graphs.successorListOf(reversedDAG, vertex);
//            String[] siblings = new String[successors.size() + 1];
//            for (Description sibling : successors)
//            {
//                s++;
//                siblings[s] = sibling.toString();
//                log.debug(s + " " + sibling + "\t");
//
//            }
//            if (s > 1) {
//                log.debug("\nNumber of siblings: " + s);
//                siblings[0] = "Number of siblings: " + s;
//                vertexList.add(siblings);
//            }
//            else if (s > 0)
//                System.out.println();
//
//            if (s > max_siblings) {
//                finalSiblings = successors;
//                max_siblings = s;
//            }
//            s = 0;
//
//        }
//
//    }
//
}