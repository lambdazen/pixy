package com.lambdazen.pixy;

/**
 * This interface is implemented by classes that visit pipes in a Pipeline to
 * make changes such as drop unnecessary steps.
 * 
 * @see com.lambdazen.pixy.postprocess.Phase1
 * @see com.lambdazen.pixy.postprocess.Phase2
 */
public interface PipeVisitor {
	public void visit(PixyPipe pp);

	public void mark();
	
	public void reset();
}
