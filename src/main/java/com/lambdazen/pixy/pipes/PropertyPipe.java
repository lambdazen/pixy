package com.lambdazen.pixy.pipes;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.gremlin.GraphTraversalExt;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Property;

public class PropertyPipe implements PixyPipe, NamedInputPipe, NamedOutputPipe {
    private String inStep;
    private String key;
    private String outStep;
    private Object defaultValue;

    public PropertyPipe(String namedStep, String key, String varName, Object defaultValue) {
        this.inStep = namedStep;
        this.key = key;
        this.outStep = varName;
        this.defaultValue = defaultValue;
    }

    public String toString() {
        return ((inStep == null) ? "" : "coalesce('" + inStep + "') -> ")
                + "property('" + key + "')"
                + (defaultValue == null
                        ? " -> filter(this != null)"
                        : " -> transform(this == null ? " + defaultValue + " : this)")
                + ((outStep == null) ? "" : " -> as('" + outStep + "')");
    }

    @Override
    public GraphTraversal pixyStep(GraphTraversal inputPipe) {
        GraphTraversal ans = inputPipe;

        if (inStep != null) {
            ans = GraphTraversalExt.coalesce(ans, inStep);
        }

        if (defaultValue == null) {
            // Without default
            ans = ans.properties(key).value().filter(new Predicate<Traverser<Property>>() {
                @Override
                public boolean test(Traverser x) {
                    return x.get() != null;
                }
            });
        } else {
            // With default
            ans = ans.optional(__.properties(key)).map(new Function<Traverser, Object>() {
                @Override
                public Object apply(Traverser t) {
                    Object x = t.get();
                    if (x instanceof Property) {
                        return ((Property) x).value();
                    } else {
                        return defaultValue;
                    }
                }
            });
        }

        if (outStep != null) {
            ans = ans.as(outStep);
        }

        return ans;
    }

    @Override
    public String getInputNamedStep() {
        return inStep;
    }

    @Override
    public void clearInputNamedStep() {
        this.inStep = null;
    }

    @Override
    public String getOutputNamedStep() {
        return outStep;
    }

    @Override
    public void clearOutputNamedStep() {
        this.outStep = null;
    }

    @Override
    public void visit(PipeVisitor visitor) {
        visitor.visit(this);
    }
}
