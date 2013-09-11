package com.lambdazen.pixy.pipes;

import java.util.Set;

import com.lambdazen.pixy.PixyPipe;

public class PipeUtils {
	public static void addDependentNamedSteps(Set<String> set, PixyPipe pp) {
		if (pp instanceof NamedInputPipe) {
			String inputStep = ((NamedInputPipe)pp).getInputNamedStep();
			if (inputStep != null) {
				set.add(inputStep);
			}
		}
		
		if (pp instanceof InternalLookupPipe) {
			set.addAll(((InternalLookupPipe)pp).getDependentNamedSteps());
		}
	}
}
