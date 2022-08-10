package customFunctions;

import org.junit.jupiter.api.*;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class BasicCustomProcedureTests {

    static Driver driver;

    @BeforeAll
    static void setup_db() {
        Neo4j neo4j = Neo4jBuilders.newInProcessBuilder()
                .withProcedure(BasicCustomProcedure.class)
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
                "merge(L)-[:RELATION]->(DS)";
        try (Session session = driver.session()) {
            session.run("match(n) detach delete n");
            session.run(cypher);
        }
    }

    @Test
    @DisplayName("Test Basic Traverse Graph Algorithm Scenarios")
    void test() {
        String cypher = "match (a:External {name: 'A'}) " +
                "call custom.traverseGraph(a, 'sens_value') yield nodes " +
                "return nodes ";
        List<Node> result = getCypherResults(cypher);
        List<String> listOfIds = result.stream().map(n -> n.get("name").asString()).collect(Collectors.toList());
        Assertions.assertIterableEquals(Arrays.asList("B", "G", "H"), listOfIds);
    }

    private List<Node> getCypherResults(String cypher) {
        try (Session session = driver.session()) {
            Result result = session.run(cypher);
            return result.single().get("nodes").asList(Value::asNode);
        }
    }
}
