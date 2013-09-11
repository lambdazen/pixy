package com.lambdazen.pixy.pipes;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.gremlin.GremlinPipelineExt;
import com.tinkerpop.gremlin.java.GremlinPipeline;

public class LoadOutputPipe implements PixyPipe, NamedInputPipe {
	private String namedStep;

	public LoadOutputPipe(String namedStep) {
		this.namedStep = namedStep;
	}
	
	public String toString() {
		return (namedStep == null) ? "" : "coalesce('" + namedStep + "')";
	}

	@Override
	public GremlinPipeline pixyStep(GremlinPipeline inputPipe) {
		if (namedStep == null) {
			return inputPipe;
		} else {
			return GremlinPipelineExt.coalesce(inputPipe, namedStep);
		}
	}

	@Override
	public String getInputNamedStep() {
		return namedStep;
	}

	@Override
	public void clearInputNamedStep() {
		this.namedStep = null;
	}

	@Override
	public void visit(PipeVisitor visitor) {
		visitor.visit(this);
	}
}
