package com.lambdazen.pixy.gremlin;

import com.lambdazen.pixy.PixyDatum;
import java.util.List;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

public class GraphTraversalExt {
    /** Method to add a CoalescePipe with one named step. Emits the value of the named step, like select(stepName) */
    public static GraphTraversal coalesce(GraphTraversal pipeline, String stepName) {
        return coalesce(pipeline, new String[] {stepName});
    }

    /** Method to add a CoalescePipe with multiple named steps. Emits the first non-null valued named step. */
    public static <S, T, E> GraphTraversal coalesce(GraphTraversal pipeline, String[] stepNames) {
        return pipeline.asAdmin().addStep(new PixyCoalesceStep(pipeline.asAdmin(), stepNames));
    }

    /** Method to add a PixySplitMerge pipe */
    public static GraphTraversal pixySplitMerge(GraphTraversal pipeline, List<GraphTraversal> pipes) {
        return pipeline.asAdmin().addStep(new PixySplitMergeStep(pipeline.asAdmin(), pipes));
    }

    /** Method to add a PixyEval pipe */
    public static GraphTraversal pixyEval(GraphTraversal pipeline, List<PixyDatum> ops) {
        return pipeline.asAdmin().addStep(new PixyEvalStep(pipeline.asAdmin(), ops));
    }

    public static GraphTraversal pixyCut(GraphTraversal pipeline) {
        return pipeline.asAdmin().addStep(new PixyCutStep(pipeline.asAdmin()));
    }
}
