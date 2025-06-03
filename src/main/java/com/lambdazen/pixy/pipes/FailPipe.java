package com.lambdazen.pixy.pipes;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import java.util.function.Predicate;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

public class FailPipe implements PixyPipe {
    public FailPipe() {}

    public String toString() {
        return "fail";
    }

    @Override
    public GraphTraversal pixyStep(GraphTraversal inputPipe) {
        return inputPipe.filter(new Predicate<Traverser>() {
            @Override
            public boolean test(Traverser ignore) {
                return false;
            }
        });
    }

    @Override
    public void visit(PipeVisitor visitor) {
        visitor.visit(this);
    }
}
