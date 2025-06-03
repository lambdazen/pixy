package com.lambdazen.pixy.pipes;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.gremlin.GraphTraversalExt;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

public class LabelPipe implements PixyPipe, NamedInputPipe, NamedOutputPipe {
    private String inStep;
    private String outStep;

    public LabelPipe(String namedStep, String varName) {
        this.inStep = namedStep;
        this.outStep = varName;
    }

    public String toString() {
        return ((inStep == null) ? "" : "coalesce('" + inStep + "') -> ")
                + "label()"
                + ((outStep == null) ? "" : " -> as('" + outStep + "')");
    }

    @Override
    public GraphTraversal pixyStep(GraphTraversal inputPipe) {
        GraphTraversal ans = inputPipe;

        if (inStep != null) {
            ans = GraphTraversalExt.coalesce(ans, inStep);
        }

        ans = ans.label();

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
