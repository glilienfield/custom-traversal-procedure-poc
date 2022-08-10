package utilities;

import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Rel {
    public String startNodeName;
    public String endNodeName;
    public String type;

    public Rel(String startNodeName, String endNodeName, String type) {
        this.startNodeName = startNodeName;
        this.endNodeName = endNodeName;
        this.type = type;
    }

    public static List<Rel> merge(Map<String, Node> nodes, List<Relationship> relationships) {
        return relationships.stream().map(r -> {
            String startNodeName = nodes.get(String.valueOf(r.startNodeId())).get("name").asString();
            String endNodeName = nodes.get(String.valueOf(r.endNodeId())).get("name").asString();
            return new Rel(startNodeName, endNodeName, r.type());
        }).collect(Collectors.toList());
    }

    public static Rel of(String startNodeName, String endNodeName, String type) {
        return new Rel(startNodeName, endNodeName, type);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Rel) {
            Rel rel = (Rel) obj;
            return (obj instanceof Rel)
                    && this.startNodeName.equals(rel.startNodeName)
                    && this.endNodeName.equals(rel.endNodeName)
                    && this.type.equals(rel.type);
        } else {
            return false;
        }
    }
}
