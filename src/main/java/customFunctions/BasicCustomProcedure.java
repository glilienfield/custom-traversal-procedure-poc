package customFunctions;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class BasicCustomProcedure {

    @Procedure(name = "custom.traverseGraph")
    @Description("Get list of nodes from root node that traverse the path with highest values of property 'prop'")
    public Stream<TraversalResult> traverseTree(@Name("root") Node rootNode, @Name("property") String prop) {

        Objects.requireNonNull(rootNode);
        Objects.requireNonNull(prop);

        List<Node> listOfNodes = new ArrayList<>();
        processNode(rootNode, prop, listOfNodes);

        return Stream.of(TraversalResult.of(listOfNodes));
    }

    private void processNode(Node node, String property, List<Node> nodes) {
        boolean nodeFound = false;
        Long currentMaxValue = Long.MIN_VALUE;
        Node childNodeWithMaxValue = null;
        Iterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING);
        for (Relationship relationship : relationships) {
            Node childNode = relationship.getOtherNode(node);
            if (childNode.hasProperty(property)) {
                Long propertyValue = (Long) childNode.getProperty(property);
                if (propertyValue > currentMaxValue) {
                    childNodeWithMaxValue = childNode;
                    currentMaxValue = propertyValue;
                    nodeFound = true;
                }
            }
        }
        if (nodeFound) {
            nodes.add(childNodeWithMaxValue);
            processNode(childNodeWithMaxValue, property, nodes);
        }
    }

    public static class TraversalResult {
        public List<Node> nodes;

        private TraversalResult(List<Node> nodes) {
            this.nodes = nodes;
        }

        public static TraversalResult of(List<Node> nodes) {
            return new TraversalResult(nodes);
        }
    }
}
