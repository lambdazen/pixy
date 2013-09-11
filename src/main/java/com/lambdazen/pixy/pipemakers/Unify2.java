package com.lambdazen.pixy.pipemakers;

import java.util.List;
import java.util.Map;

import com.lambdazen.pixy.PipeMaker;
import com.lambdazen.pixy.PixyDatum;
import com.lambdazen.pixy.PixyGrinder;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.VariableGenerator;
import com.lambdazen.pixy.pipes.NoopPipe;

public class Unify2 implements PipeMaker {
	@Override
	public String getSignature() {
		return "(=)/2";
	}

	@Override
	public PixyPipe makePipe(List<PixyDatum> bindings, Map<String, PixyDatum> replacements, VariableGenerator varGen) {
		// Using the internal unification scheme
		PixyDatum binding1 = bindings.get(0);
		PixyDatum binding2 = bindings.get(1);
		
		// One of the bindings must be ground -- the other one is the target
		PixyPipe ans;
		if (binding2.isGround()) {
			ans = PixyGrinder.unifyVar(replacements, null, binding1, binding2);
		} else if (binding1.isGround()) {
			ans = PixyGrinder.unifyVar(replacements, null, binding2, binding1);
		} else {
			// Can't unify two variables in a pipe -- need to find another sequence
			return null;
		}

		if (ans == null) {
			return new NoopPipe();
		} else {
			return ans;
		}
	}
}
