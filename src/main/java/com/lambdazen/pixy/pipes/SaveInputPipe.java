package com.lambdazen.pixy.pipes;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.tinkerpop.gremlin.java.GremlinPipeline;

public class SaveInputPipe implements PixyPipe, NamedOutputPipe {
	private String namedStep;

	public SaveInputPipe(String namedStep) {
		this.namedStep = namedStep;
	}
	
	public String toString() {
		return (namedStep == null) ? "" : "as('" + namedStep + "')";
	}

	@Override
	public GremlinPipeline pixyStep(GremlinPipeline inputPipe) {
		if (namedStep == null) {
			return inputPipe;
		} else {
			return inputPipe.as(namedStep);
		}
	}

	@Override
	public String getOutputNamedStep() {
		return namedStep;
	}

	@Override
	public void clearOutputNamedStep() {
		this.namedStep = null;
	}

	@Override
	public void visit(PipeVisitor visitor) {
		visitor.visit(this);
	}
}
