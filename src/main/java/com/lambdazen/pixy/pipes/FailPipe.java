package com.lambdazen.pixy.pipes;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;

public class FailPipe implements PixyPipe {
	public FailPipe() {
		
	}
	
	public String toString() {
		return "fail";
	}

	@Override
	public GremlinPipeline pixyStep(GremlinPipeline inputPipe) {
		return inputPipe.filter(new PipeFunction() {
			@Override
			public Boolean compute(Object x) {
				return false;
			}
		});
	}

	@Override
	public void visit(PipeVisitor visitor) {
		visitor.visit(this);
	}
}
