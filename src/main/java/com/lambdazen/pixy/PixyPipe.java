package com.lambdazen.pixy;

import com.tinkerpop.gremlin.java.GremlinPipeline;

/**
 * This interface is implemented by pipes in Pixy. The pixyStep() method is used
 * to create a GremlinPipeline. The toString() method should show the operations
 * inside the pipe.
 */
public interface PixyPipe {
	public GremlinPipeline pixyStep(GremlinPipeline inputPipe);
	
	public void visit(PipeVisitor visitor);
}
