package com.lambdazen.pixy.pipes;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

public class NoopPipe implements PixyPipe {
    @Override
    public GraphTraversal pixyStep(GraphTraversal inputPipe) {
        return inputPipe;
    }

    @Override
    public void visit(PipeVisitor visitor) {
        // Skip this one
    }

    public String toString() {
        return "";
    }
}
