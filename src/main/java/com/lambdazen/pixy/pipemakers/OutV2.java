package com.lambdazen.pixy.pipemakers;

import java.util.List;
import java.util.Map;

import com.lambdazen.pixy.PipeMaker;
import com.lambdazen.pixy.PixyDatum;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.VariableGenerator;
import com.lambdazen.pixy.pipes.AdjacentStep;

public class OutV2 implements PipeMaker { 
	@Override
	public String getSignature() {
		return "outV/2";
	}

	@Override
	public PixyPipe makePipe(List<PixyDatum> bindings, Map<String, PixyDatum> replacements, VariableGenerator varGen) {
		return PipeMakerUtils.adjacentStepPipe(AdjacentStep.outV, AdjacentStep.inV, bindings, replacements);
	}
}
