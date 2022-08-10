package customFunctions;

import org.junit.jupiter.api.*;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import utilities.Rel;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    @DisplayName("Test Advanced Traverse Graph Algorithm Scenarios")
    @Nested
    class NoListScenarios {
        @Test
        void test_list_of_nodes_is_correct() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            Map<String, Node> nodes = getNodeMap(result);

            Assertions.assertEquals(3, nodes.size());
            Assertions.assertIterableEquals(Arrays.asList("A", "DS", "X"), nodes.values().stream().map(v -> v.get("name").asString()).sorted().collect(Collectors.toList()));
        }

        @Test
        void test_list_of_relationships_is_correct() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(2, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "X", "BLACK_LIST"));
            Assertions.assertEquals(rel.get(1), Rel.of("X", "DS", "RELATION"));
        }

        @Test
        void test_with_max_depth_equal_to_zero_should_return_empty_list_and_map() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {maxDepth: 0}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            Map<String, Node> nodes = getNodeMap(result);
            List<Relationship> relationships = getRelationshipList(result);

            Assertions.assertEquals(0, nodes.size());
            Assertions.assertEquals(0, relationships.size());
        }
    }

    @DisplayName("Test Node White List Scenarios")
    @Nested
    class TestNodeWhiteListScenarios {
        @Test
        void test_with_white_list_as_single_value_of_Node() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {nodes: {whiteList: 'Node'}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(4, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "B", "RELATION"));
            Assertions.assertEquals(rel.get(1), Rel.of("B", "G", "RELATION"));
            Assertions.assertEquals(rel.get(2), Rel.of("G", "H", "RELATION"));
            Assertions.assertEquals(rel.get(3), Rel.of("H", "DS", "RELATION"));
        }

        @Test
        void test_with_white_list_as_a_list_with_Node_Label() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {nodes: {whiteList: ['Node', 'Other']}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(4, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "B", "RELATION"));
            Assertions.assertEquals(rel.get(1), Rel.of("B", "G", "RELATION"));
            Assertions.assertEquals(rel.get(2), Rel.of("G", "H", "RELATION"));
            Assertions.assertEquals(rel.get(3), Rel.of("H", "DS", "RELATION"));
        }
    }

    @DisplayName("Test Node Black List Scenarios")
    @Nested
    class TestNodeBlackListScenarios {
        @Test
        void test_with_black_list_as_a_single_value_of_Node() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {nodes: {blackList: 'Node'}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(2, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "X", "BLACK_LIST"));
            Assertions.assertEquals(rel.get(1), Rel.of("X", "DS", "RELATION"));
        }

        @Test
        void test_with_black_list_as_a_single_value_of_BlackList() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {nodes: {blackList: 'BlackList'}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(4, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "B", "RELATION"));
            Assertions.assertEquals(rel.get(1), Rel.of("B", "G", "RELATION"));
            Assertions.assertEquals(rel.get(2), Rel.of("G", "H", "RELATION"));
            Assertions.assertEquals(rel.get(3), Rel.of("H", "DS", "RELATION"));
        }

        @Test
        void test_with_black_list_as_a_list_with_Node_Label() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {nodes: {blackList: ['Node', 'Other']}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(2, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "X", "BLACK_LIST"));
            Assertions.assertEquals(rel.get(1), Rel.of("X", "DS", "RELATION"));
        }
    }

    @DisplayName("Test Relationship White List Scenarios")
    @Nested
    class TestRelationshipWithListScenarios {
        @Test
        void test_with_black_list_as_a_single_value_of_RELATION() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {relationships: {whiteList: 'BLACK_LIST'}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(2, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "X", "BLACK_LIST"));
            Assertions.assertEquals(rel.get(1), Rel.of("X", "DS", "RELATION"));
        }

        @Test
        void test_with_black_list_as_a_single_value_of_BLACK_LIST() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {relationships: {whiteList: 'RELATION'}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(4, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "B", "RELATION"));
            Assertions.assertEquals(rel.get(1), Rel.of("B", "G", "RELATION"));
            Assertions.assertEquals(rel.get(2), Rel.of("G", "H", "RELATION"));
            Assertions.assertEquals(rel.get(3), Rel.of("H", "DS", "RELATION"));
        }

        @Test
        void test_with_black_list_as_a_list_with_BLACK_LIST() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {relationships: {whiteList: ['RELATION', 'OTHER']}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(4, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "B", "RELATION"));
            Assertions.assertEquals(rel.get(1), Rel.of("B", "G", "RELATION"));
            Assertions.assertEquals(rel.get(2), Rel.of("G", "H", "RELATION"));
            Assertions.assertEquals(rel.get(3), Rel.of("H", "DS", "RELATION"));
        }
    }

    @DisplayName("Test Relationship Black List Scenarios")
    @Nested
    class TestRelationshipBlackListScenarios {
        @Test
        void test_with_black_list_as_a_single_value_of_RELATION() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {relationships: {blackList: 'RELATION'}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(2, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "X", "BLACK_LIST"));
            Assertions.assertEquals(rel.get(1), Rel.of("X", "DS", "RELATION"));
        }

        @Test
        void test_with_black_list_as_a_single_value_of_BLACK_LIST() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {relationships: {blackList: 'BLACK_LIST'}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(4, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "B", "RELATION"));
            Assertions.assertEquals(rel.get(1), Rel.of("B", "G", "RELATION"));
            Assertions.assertEquals(rel.get(2), Rel.of("G", "H", "RELATION"));
            Assertions.assertEquals(rel.get(3), Rel.of("H", "DS", "RELATION"));
        }

        @Test
        void test_with_black_list_as_a_list_with_BLACK_LIST() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {relationships: {blackList: ['BLACK_LIST', 'OTHER']}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(4, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "B", "RELATION"));
            Assertions.assertEquals(rel.get(1), Rel.of("B", "G", "RELATION"));
            Assertions.assertEquals(rel.get(2), Rel.of("G", "H", "RELATION"));
            Assertions.assertEquals(rel.get(3), Rel.of("H", "DS", "RELATION"));
        }
    }

    @DisplayName("Test Mixed List Scenarios")
    @Nested
    class TestMixedListScenarios {
        @Test
        void test_with_white_lists() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {nodes: {whiteList: 'Node', blackList: 'BlackList'}, relationships: {whiteList: 'RELATION', blackList: 'BLACK_LIST'}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(4, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "B", "RELATION"));
            Assertions.assertEquals(rel.get(1), Rel.of("B", "G", "RELATION"));
            Assertions.assertEquals(rel.get(2), Rel.of("G", "H", "RELATION"));
            Assertions.assertEquals(rel.get(3), Rel.of("H", "DS", "RELATION"));
        }

        @Test
        void test_with_black_lists() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {nodes: {blackList: 'BlackList'}, relationships: {blackList: 'BLACK_LIST'}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(4, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "B", "RELATION"));
            Assertions.assertEquals(rel.get(1), Rel.of("B", "G", "RELATION"));
            Assertions.assertEquals(rel.get(2), Rel.of("G", "H", "RELATION"));
            Assertions.assertEquals(rel.get(3), Rel.of("H", "DS", "RELATION"));
        }
    }

    @DisplayName("Test Max Depth Scenarios")
    @Nested
    class TestMaxDepthScenarios {
        @Test
        void test_with_a_max_depth_greater_than_the_path_length_so_no_truncating() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {maxDepth: 5, relationships: {whiteList: 'RELATION'}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(4, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "B", "RELATION"));
            Assertions.assertEquals(rel.get(1), Rel.of("B", "G", "RELATION"));
            Assertions.assertEquals(rel.get(2), Rel.of("G", "H", "RELATION"));
            Assertions.assertEquals(rel.get(3), Rel.of("H", "DS", "RELATION"));
        }

        @Test
        void test_with_a_max_depth_equal_to_the_path_length_so_no_truncating() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {maxDepth: 4, relationships: {whiteList: 'RELATION'}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(4, rel.size());
            Assertions.assertEquals(rel.get(0), Rel.of("A", "B", "RELATION"));
            Assertions.assertEquals(rel.get(1), Rel.of("B", "G", "RELATION"));
            Assertions.assertEquals(rel.get(2), Rel.of("G", "H", "RELATION"));
            Assertions.assertEquals(rel.get(3), Rel.of("H", "DS", "RELATION"));
        }

        @Test
        void test_with_a_max_depth_less_than_the_path_length_so_no_truncating() {
            String cypher = "match (a:External {name: 'A'}) " +
                    "match (ds:Node {name:'DS'}) " +
                    "call custom.advancedTraverseGraph(a, ds, 'sens_value', {maxDepth: 3, relationships: {whiteList: 'RELATION'}}) yield relationships, nodes " +
                    "return relationships, nodes ";

            Record result = getCypherResults(cypher);
            List<Rel> rel = getResult(result);

            Assertions.assertEquals(0, result.get("nodes").asMap().size());
            Assertions.assertEquals(0, result.get("relationships").asList().size());
        }
    }

    private List<Relationship> getRelationshipList(Record record) {
        return record.get("relationships").asList(Value::asRelationship);
    }

    private Map<String, Node> getNodeMap(Record record) {
        return record.get("nodes").asMap(Value::asNode);

    }

    private List<Rel> getResult(Record record) {
        Map<String, Node> nodes = getNodeMap(record);
        List<Relationship> relationships = getRelationshipList(record);
        return Rel.merge(nodes, relationships);
    }

    private Record getCypherResults(String cypher) {
        try (Session session = driver.session()) {
            Result result = session.run(cypher);
            return result.single();
        }
    }
}


//    @Test
//    void test_with_max_depth_equal_to_0_should_return_empty_list_and_map() {
//        String cypher = "match (a:External {name: 'A'}) " +
//                "match (ds:Node {name:'DS'}) " +
//                "call custom.advancedTraverseGraph(a, ds, 'sens_value', {maxDepth: 0}) yield relationships, nodes " +
//                "return relationships, nodes ";
//
//        Record result = getCypherResults(cypher);
//        Map<String, Node> nodes = getNodeMap(result);
//        List<Relationship> relationships = getRelationshipList(result);
//
//        Assertions.assertEquals(0, nodes.size());
//        Assertions.assertEquals(0, relationships.size());
//    }
