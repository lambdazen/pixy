package com.lambdazen.pixy.gremlin;

import com.tinkerpop.pipes.filter.RangeFilterPipe;
import com.tinkerpop.pipes.util.PipeHelper;

public class PixyCutPipe<S> extends RangeFilterPipe<S> {
	boolean wasCut;
	
	public PixyCutPipe() {
		super(0, 0);
		this.wasCut = false;
	}
	
	public S processNextStart() {
		S ans = super.processNextStart();
		
		// Successful -- so cut has been triggered
		wasCut = true;
		
		return ans;
	}
	
	public boolean wasCut() {
		return wasCut;
	}
	
	public void reset() {
		super.reset();

		this.wasCut = false;
	}

	public String toString() {
    	return PipeHelper.makePipeString(this);
    }
}
