package com.lambdazen.pixy.pipes;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.gremlin.GremlinPipelineExt;
import com.tinkerpop.gremlin.java.GremlinPipeline;

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
	public GremlinPipeline pixyStep(GremlinPipeline inputPipe) {
		GremlinPipeline ans = inputPipe;

		if (inStep != null) {
			ans = GremlinPipelineExt.coalesce(ans, inStep);
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
