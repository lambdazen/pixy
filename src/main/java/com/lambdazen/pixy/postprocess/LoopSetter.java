package com.lambdazen.pixy.postprocess;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.pipes.AdjacentStepPipe;
import com.lambdazen.pixy.pipes.LoopDetails;

public class LoopSetter implements PipeVisitor {
	private LoopDetails loopDetails;
	private boolean wasSet = false;
	
	public LoopSetter(LoopDetails loopDetails) { 
		this.loopDetails = loopDetails;
	}

	@Override
	public void visit(PixyPipe pp) {
		if (pp instanceof AdjacentStepPipe) {
			assert !wasSet : "LoopSetter should not set more than one AdjacentStepPipe";

			AdjacentStepPipe pipe = (AdjacentStepPipe)pp;
			
			pipe.setLoopDetails(loopDetails);
			
			wasSet = true;
		}
	}

	@Override
	public void mark() {
		// Nothing to do
	}

	@Override
	public void reset() {
		// Nothing to do
	}
}
