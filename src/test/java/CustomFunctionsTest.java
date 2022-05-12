import com.glilienfield.CustomFunctions;
import org.junit.jupiter.api.*;
import org.neo4j.driver.*;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

class CustomFunctionsTest {

    static Driver driver;

    @BeforeAll
    static void setup_db() {
        Neo4j neo4j = Neo4jBuilders.newInProcessBuilder()
                .withFunction(CustomFunctions.class)
                .build();

        driver = GraphDatabase.driver(neo4j.boltURI(), Config.builder()
                .withoutEncryption()
                .build());
    }

    @BeforeEach
    void delete_data() {
        try (Session session = driver.session()) {
            session.run("match(n) detach delete n");
        }
    }

    @Test
    @DisplayName("a has null list, b has null list")
    void test_both_nodes_have_null_lists() {
        String cypher = "create (a{id: 1}), (b{id: 2})";
        boolean isSimilar = executeTest(cypher);
        Assertions.assertFalse(isSimilar);
    }

    @Test
    @DisplayName("a has null list, b has non-null list")
    void test_a_node_has_null_lists_b_node_has_non_null_list() {
        String cypher = "create (a{id: 1}), (b{id: 2, list_extern: ['1','2']})";
        boolean isSimilar = executeTest(cypher);
        Assertions.assertFalse(isSimilar);
    }

    @Test
    @DisplayName("a has non-null list, b has null list")
    void test_a_node_has_non_null_lists_b_node_has_null_list() {
        String cypher = "create (a{id: 1, list_intern: ['1','2']}), (b{id: 2})";
        boolean isSimilar = executeTest(cypher);
        Assertions.assertFalse(isSimilar);
    }

    @Test
    @DisplayName("a and b have lists of integers with no common elements")
    void test_a_and_b_have_lists_with_no_common_elements() {
        String cypher = "create (a{id: 1, list_intern: ['10','20']}), (b{id: 2, list_extern: ['1','2']})";
        boolean isSimilar = executeTest(cypher);
        Assertions.assertFalse(isSimilar);
    }

    @Test
    @DisplayName("a and b have lists of integers with one common element")
    void test_a_and_b_have_lists_with_one_common_element() {
        String cypher = "create (a{id: 1, list_intern: ['10','2']}), (b{id: 2, list_extern: ['1','2']})";
        boolean isSimilar = executeTest(cypher);
        Assertions.assertTrue(isSimilar);
    }

    @Test
    @DisplayName("a and b have lists of integers with multiple common element")
    void test_a_and_b_have_lists_with_multiple_common_element() {
        String cypher = "create (a{id: 1, list_intern: ['10','2', '100']}), (b{id: 2, list_extern: ['100','1','2']})";
        boolean isSimilar = executeTest(cypher);
        Assertions.assertTrue(isSimilar);
    }

    @Test
    @DisplayName("a has a single element and b has a list that contains the single element in a")
    void test_a_has_a_single_element_and_b_has_a_list_that_contains_the_element_of_a() {
        String cypher = "create (a{id: 1, list_intern: '2'}), (b{id: 2, list_extern: ['100','1','2']})";
        boolean isSimilar = executeTest(cypher);
        Assertions.assertTrue(isSimilar);
    }

    @Test
    @DisplayName("b has a single element and a has a list that contains the single element in b")
    void test_b_has_a_single_element_and_a_has_a_list_that_contains_the_element_of_b() {
        String cypher = "create (a{id: 1, list_intern: ['100','1','2']}), (b{id: 2, list_extern: '2'})";
        boolean isSimilar = executeTest(cypher);
        Assertions.assertTrue(isSimilar);
    }

    @Test
    @DisplayName("a has a single element and b has the single element in b")
    void test_a_has_a_single_element_and_b_has_the_single_element_of_b() {
        String cypher = "create (a{id: 1, list_intern: '2'}), (b{id: 2, list_extern: '2'})";
        boolean isSimilar = executeTest(cypher);
        Assertions.assertTrue(isSimilar);
    }

    @Test
    @DisplayName("a and b have single elements that are not equal")
    void test_a_and_b_have_a_single_element_that_are_not_equal() {
        String cypher = "create (a{id: 1, list_intern: '2'}), (b{id: 2, list_extern: '4'})";
        boolean isSimilar = executeTest(cypher);
        Assertions.assertFalse(isSimilar);
    }

    private boolean executeTest(String setupCypher) {
        try (Session session = driver.session()) {
            session.run(setupCypher);
            Result result = session.run("match (a{id: 1}), (b{id: 2}) return com.glilienfield.isSimilar('list_intern', a, 'list_extern', b) as result");
            if (result.hasNext()) {
                Record record = result.next();
                return record.get("result").asBoolean();
            } else {
                throw new IllegalArgumentException();
            }
        }
    }
}
