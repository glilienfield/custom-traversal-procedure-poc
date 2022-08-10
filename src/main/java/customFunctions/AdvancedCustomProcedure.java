package customFunctions;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class AdvancedCustomProcedure {

    @Procedure(name = "custom.advancedTraverseGraph")
    @Description("Get path from root node to terminating node that traverses the path with highest values of property 'prop'")
    public Stream<TraversalResult> advanceTraverseTree(@Name("root") Node rootNode,
                                                       @Name("terminal") Node terminalNode,
                                                       @Name("property") String prop,
                                                       @Name("config") Map<String, Object> config) {

        Objects.requireNonNull(rootNode);
        Objects.requireNonNull(terminalNode);
        Objects.requireNonNull(prop);

        Long maxDepth = (config != null && config.containsKey("maxDepth")) ? (Long) config.get("maxDepth") : Long.MAX_VALUE;
        List<String> nodeWhiteList = extractListFromConfig(config, "nodes", "whiteList");
        List<String> nodeBlackList = extractListFromConfig(config, "nodes", "blackList");
        List<String> relationshipWhiteList = extractListFromConfig(config, "relationships", "whiteList");
        List<String> relationshipBlackList = extractListFromConfig(config, "relationships", "blackList");

        List<Relationship> listOfRelationships = new ArrayList<>();
        Map<String, Node> mapOfNodes = new HashMap<>();
        mapOfNodes.put(String.valueOf(rootNode.getId()), rootNode);

        processNode(rootNode, terminalNode, prop, listOfRelationships, mapOfNodes, maxDepth, nodeWhiteList, nodeBlackList, relationshipWhiteList, relationshipBlackList, 0);

        return Stream.of(TraversalResult.of(listOfRelationships, mapOfNodes));
    }

    private void processNode(Node rootNode,
                             Node terminalNode,
                             String property,
                             List<Relationship> listOfRelationships,
                             Map<String, Node> mapOfNodes,
                             long maxDepth,
                             List<String> nodeWhiteList,
                             List<String> nodeBlackList,
                             List<String> relationshipWhiteList,
                             List<String> relationshipBlackList,
                             long level) {
        if (level > maxDepth) {
            listOfRelationships.clear();
            mapOfNodes.clear();
            return;
        }
        boolean nodeFound = false;
        boolean terminated = false;
        Long currentMaxValue = Long.MIN_VALUE;
        Node chosenChildNode = null;
        Relationship chosenChildRelationship = null;
        Iterable<Relationship> relationships = rootNode.getRelationships(Direction.OUTGOING);
        for (Relationship relationship : relationships) {
            Node childNode = relationship.getOtherNode(rootNode);
            List<String> nodeLabels = StreamSupport.stream(childNode.getLabels().spliterator(), false).map(Label::name).collect(Collectors.toList());
            String relType = relationship.getType().name();
            if ((relationshipWhiteList.isEmpty() || relationshipWhiteList.contains(relType))
                    && (relationshipBlackList.isEmpty() || !relationshipBlackList.contains(relType))
                    && (nodeWhiteList.isEmpty() || nodeLabels.stream().anyMatch(nodeWhiteList::contains))
                    && (nodeBlackList.isEmpty() || nodeLabels.stream().noneMatch(nodeBlackList::contains))) {
                if (childNode.getId() != terminalNode.getId()) {
                    if (childNode.hasProperty(property)) {
                        Long propertyValue = (Long) childNode.getProperty(property);
                        if (propertyValue > currentMaxValue) {
                            chosenChildNode = childNode;
                            chosenChildRelationship = relationship;
                            currentMaxValue = propertyValue;
                            nodeFound = true;
                        }
                    }
                } else {
                    listOfRelationships.add(relationship);
                    mapOfNodes.put(String.valueOf(terminalNode.getId()), terminalNode);
                    terminated = true;
                    break;
                }
            }
        }
        if (nodeFound && !terminated) {
            listOfRelationships.add(chosenChildRelationship);
            mapOfNodes.put(String.valueOf(chosenChildNode.getId()), chosenChildNode);
            processNode(chosenChildNode, terminalNode, property, listOfRelationships, mapOfNodes, maxDepth, nodeWhiteList, nodeBlackList, relationshipWhiteList, relationshipBlackList, level + 1);
        }
    }

    private List<String> extractListFromConfig(Map<String, Object> config, String type, String list) {
        if (config != null && config.containsKey(type)) {
            Map<String, Object> map = (Map<String, Object>) config.get(type);
            if (map.containsKey(list)) {
                Object x = map.get(list);
                if (x instanceof List) {
                    return (List<String>) x;
                } else {
                    return Arrays.asList((String) x);
                }
            } else {
                return Collections.emptyList();
            }
        } else {
            return Collections.emptyList();
        }
    }

    public static class TraversalResult {
        public List<Relationship> relationships;
        public Map<String, Node> nodes;

        private TraversalResult(List<Relationship> listOfRelationships, Map<String, Node> nodes) {
            this.relationships = listOfRelationships;
            this.nodes = nodes;
        }

        public static TraversalResult of(List<Relationship> listOfRelationships, Map<String, Node> nodes) {
            return new TraversalResult(listOfRelationships, nodes);
        }
    }
}
