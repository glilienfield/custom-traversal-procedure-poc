package customFunctions;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.*;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

class AdvancedCustomProcedureTest {
    static Driver driver;

    @BeforeAll
    static void setup_db() {
        Neo4j neo4j = Neo4jBuilders.newInProcessBuilder()
                .withProcedure(AdvancedCustomProcedure.class)
                .build();

        driver = GraphDatabase.driver(neo4j.boltURI(), Config.builder()
                .withoutEncryption()
                .build());
    }

    @BeforeEach
    void delete_data() {
        String cypher = "merge(A:External {name:'A'})\n" +
                "merge(B:Node {name:'B'}) set B.sens_value = 80\n" +
                "merge(C:Node {name:'C'}) set C.sens_value = 70\n" +
                "merge(D:Node {name:'D'}) set D.sens_value = 50\n" +
                "merge(E:Node {name:'E'}) set E.sens_value = 90\n" +
                "merge(F:Node {name:'F'}) set F.sens_value = 10\n" +
                "merge(G:Node {name:'G'}) set G.sens_value = 40\n" +
                "merge(H:Node {name:'H'}) set H.sens_value = 60\n" +
                "merge(I:Node {name:'I'}) set I.sens_value = 50\n" +
                "merge(J:Node {name:'J'}) set J.sens_value = 30\n" +
                "merge(K:Node {name:'K'}) set K.sens_value = 45\n" +
                "merge(L:Node {name:'L'}) set L.sens_value = 55\n" +
                "merge(X:BlackList {name:'X'}) set X.sens_value = 100\n" +
                "merge(DS:Node {name:'DS'})\n" +
                "merge(A)-[:RELATION]->(B)\n" +
                "merge(A)-[:RELATION]->(C)\n" +
                "merge(A)-[:RELATION]->(D)\n" +
                "merge(D)-[:RELATION]->(E)\n" +
                "merge(E)-[:RELATION]->(F)\n" +
                "merge(F)-[:RELATION]->(DS)\n" +
                "merge(B)-[:RELATION]->(G)\n" +
                "merge(G)-[:RELATION]->(I)\n" +
                "merge(I)-[:RELATION]->(DS)\n" +
                "merge(G)-[:RELATION]->(H)\n" +
                "merge(H)-[:RELATION]->(DS)\n" +
                "merge(B)-[:RELATION]->(J)\n" +
                "merge(J)-[:RELATION]->(DS)\n" +
                "merge(C)-[:RELATION]->(K)\n" +
                "merge(K)-[:RELATION]->(L)\n" +
                "merge(A)-[:BLACK_LIST]->(X)\n" +
                "merge(X)-[:RELATION]->(DS)\n" +
                "merge(L)-[:RELATION]->(DS)";
        try (Session session = driver.session()) {
            session.run("match(n) detach delete n");
            session.run(cypher);
        }
    }

    @Test
    @DisplayName("Test TraverseGraph Scenarios")
    void test() {
        String cypher = "match (a:External {name: 'A'}) " +
                "match (ds:Node {name:'DS'}) " +
                "call custom.advancedTraverseGraph(a, ds, 'sens_value', {maxDepth: 10, nodes:{whiteList: ['Node', 'BlackList']}, relationships: {blackList: ['AAA']}}) yield relationships, nodes " +
                "return relationships, nodes ";
        Record result = getCypherResults(cypher);
        result.get("relationships").asList(Value::asRelationship).stream().forEach(
                r-> System.out.println(String.format("id: %s, type: %s, start node id: %s, end node id: %s", r.id(), r.type(), r.startNodeId(), r.endNodeId()))
        );
        result.get("nodes").asMap(Value::asNode).forEach(
                (k,v)-> System.out.println(String.format("node id: %s, labels: %s, name: %s, sens_value: %s", k, v.labels(), v.get("name"), v.get("sens_value")))
        );
//        Assertions.assertIterableEquals(Arrays.asList(1, 2, 3), listOfIds);
//        System.out.println(listOfIds);
//        System.out.println(mapOfNodes);
    }

    private Record getCypherResults(String cypher) {
        try (Session session = driver.session()) {
            Result result = session.run(cypher);
            return result.single();
        }
    }
}
