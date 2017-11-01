package com.lambdazen.pixy;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

/**
 * This interface is implemented by pipes in Pixy. The pixyStep() method is used
 * to create a GremlinPipeline. The toString() method should show the operations
 * inside the pipe.
 * @param <E>
 */
public interface PixyPipe {
	public GraphTraversal pixyStep(GraphTraversal inputPipe);
	
	public void visit(PipeVisitor visitor);
}
