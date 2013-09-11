package com.lambdazen.pixy.pipes;

import java.util.HashSet;
import java.util.Set;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.tinkerpop.gremlin.java.GremlinPipeline;

public class ConnectPipe implements PixyPipe, InternalLookupPipe {
	PixyPipe fromPipe;
	PixyPipe toPipe;
	
	public ConnectPipe(PixyPipe fromPipe, PixyPipe toPipe) {
		assert(fromPipe != null);
		assert(toPipe != null);

		this.fromPipe = fromPipe;
		this.toPipe = toPipe;
	}
	
	public String toString() {
		String lhs = fromPipe.toString();
		String rhs = toPipe.toString();
		if ((lhs.length() > 0) & (rhs.length() > 0)) {
			return lhs + " -> " + rhs;
		} else {
			return lhs + rhs;
		}
	}

	@Override
	public GremlinPipeline pixyStep(GremlinPipeline inputPipe) {
		GremlinPipeline ans = inputPipe;
		
		ans = fromPipe.pixyStep(ans);
		ans = toPipe.pixyStep(ans);
		
		return ans;
	}

	@Override
	public Set<String> getDependentNamedSteps() {
		Set<String> ans = new HashSet<String>();
		
		PipeUtils.addDependentNamedSteps(ans, fromPipe);
		PipeUtils.addDependentNamedSteps(ans, toPipe);
		
		return ans;
	}

	@Override
	public void visit(PipeVisitor visitor) {
		fromPipe.visit(visitor);
		toPipe.visit(visitor);
	}
}
