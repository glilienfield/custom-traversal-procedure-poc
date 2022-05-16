package customFunctions;

import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class CustomFunctions {

    @UserFunction()
    @Description("Calculate the similarity between two nodes by verifying list of strings have at least on element in common")
    public boolean isSimilar(@Name("aProp") String aProp, @Name("a") Node a, @Name("bProp") String bProp, @Name("b") Node b) {

        Objects.requireNonNull(aProp);
        Objects.requireNonNull(a);
        Objects.requireNonNull(bProp);
        Objects.requireNonNull(b);

        if(!a.hasProperty(aProp) || !b.hasProperty(bProp)) {
            return false;
        }

       Object list_intern = a.getProperty(aProp);
       Object list_extern = b.getProperty(bProp);

        boolean is_list_intern_a_list = list_intern.getClass().isArray();
        boolean is_list_extern_a_list = list_extern.getClass().isArray();

        if(is_list_intern_a_list && is_list_extern_a_list) {
            List<String> list_intern_as_list = Arrays.asList((String[]) list_intern);
            List<String> list_extern_as_list = Arrays.asList((String[]) list_extern);
            return list_intern_as_list.stream().anyMatch(list_extern_as_list::contains);
        }

        if(is_list_intern_a_list) {
            return Arrays.asList((String[])list_intern).contains(list_extern);
        }

        if(is_list_extern_a_list) {
            return Arrays.asList((String[])list_extern).contains(list_intern);
        }

        return list_intern.equals(list_extern);
    }
}
