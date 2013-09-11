package com.lambdazen.pixy.pipes;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.gremlin.GremlinPipelineExt;
import com.tinkerpop.gremlin.java.GremlinPipeline;

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
	public GremlinPipeline pixyStep(GremlinPipeline inputPipe) {
		return GremlinPipelineExt.pixyCut(inputPipe).as(outStep);
	}

	@Override
	public void visit(PipeVisitor visitor) {
		visitor.visit(this);
	}
}
