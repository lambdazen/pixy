package com.lambdazen.pixy.pipes;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.gremlin.GraphTraversalExt;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

public class CutPipe implements PixyPipe {
    // The outStep is mandatory -- so CutPipe doesn't implement NamedOutputStep
    String outStep;

    public CutPipe(String cutVarName) {
        this.outStep = cutVarName;
    }

    public String toString() {
        return "pixyCut() -> as('" + outStep + "')";
    }

    @Override
    public GraphTraversal pixyStep(GraphTraversal inputPipe) {
        return GraphTraversalExt.pixyCut(inputPipe).as(outStep);
    }

    @Override
    public void visit(PipeVisitor visitor) {
        visitor.visit(this);
    }
}
