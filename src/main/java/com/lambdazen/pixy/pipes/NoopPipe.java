package com.lambdazen.pixy.pipes;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.tinkerpop.gremlin.java.GremlinPipeline;

public class NoopPipe implements PixyPipe {
	@Override
	public GremlinPipeline pixyStep(GremlinPipeline inputPipe) {
		return inputPipe;
	}

	@Override
	public void visit(PipeVisitor visitor) {
		// Skip this one
	}
	
    public String toString() {
    	return "";
    }
}
