package com.lambdazen.pixy.gremlin;

import java.util.Collection;
import java.util.List;

import com.lambdazen.pixy.PixyDatum;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.util.AsPipe;
import com.tinkerpop.pipes.util.FluentUtility;
import com.tinkerpop.pipes.util.Pipeline;

public class GremlinPipelineExt {
	/** Method to add a CoalescePipe with one named step. Emits the value of the named step, like select(stepName) */
    public static <S, T, E>  GremlinPipeline<S, E> coalesce(GremlinPipeline<S, T> pipeline, String stepName) {
    	return pipeline.add(new CoalescePipe<T, E>(stepName, FluentUtility.getAsPipes(pipeline)));
    }

    /** Method to add a CoalescePipe with multiple named steps. Emits the first non-null valued named step. */
    public static <S, T, E> GremlinPipeline<S, E> coalesce(GremlinPipeline<S, T> pipeline, Collection<String> stepNames) {
    	List<AsPipe> asPipes = FluentUtility.getAsPipes(pipeline);
    	return pipeline.add(new CoalescePipe<T, E>(stepNames, asPipes));
    }
    
    /** Method to add a PixySplitMerge pipe */
    public static GremlinPipeline pixySplitMerge(GremlinPipeline pipeline, List<Pipeline> pipes) {
    	return pipeline.add(new PixySplitMergePipe(pipes, FluentUtility.getAsPipes(pipeline)));
    }

    /** Method to add a PixyEval pipe */
    public static GremlinPipeline pixyEval(GremlinPipeline pipeline, List<PixyDatum> ops) {
    	return pipeline.add(new PixyEvalPipe(ops, FluentUtility.getAsPipes(pipeline)));
    }

	public static GremlinPipeline pixyCut(GremlinPipeline pipeline) {
		return pipeline.add(new PixyCutPipe());
	}
}
