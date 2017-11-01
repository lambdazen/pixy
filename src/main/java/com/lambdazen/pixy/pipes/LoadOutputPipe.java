package com.lambdazen.pixy.pipes;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.gremlin.GraphTraversalExt;

public class LoadOutputPipe implements PixyPipe, NamedInputPipe {
	private String namedStep;

	public LoadOutputPipe(String namedStep) {
		this.namedStep = namedStep;
	}
	
	public String toString() {
		return (namedStep == null) ? "" : "coalesce('" + namedStep + "')";
	}

	@Override
	public GraphTraversal pixyStep(GraphTraversal inputPipe) {
		if (namedStep == null) {
			return inputPipe;
		} else {
			return GraphTraversalExt.coalesce(inputPipe, namedStep);
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
