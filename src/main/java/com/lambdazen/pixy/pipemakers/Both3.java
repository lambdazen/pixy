package com.lambdazen.pixy.pipemakers;

import com.lambdazen.pixy.PipeMaker;
import com.lambdazen.pixy.PixyDatum;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.VariableGenerator;
import com.lambdazen.pixy.pipes.AdjacentStep;
import java.util.List;
import java.util.Map;

public class Both3 implements PipeMaker {
    @Override
    public String getSignature() {
        return "both/3";
    }

    @Override
    public PixyPipe makePipe(List<PixyDatum> bindings, Map<String, PixyDatum> replacements, VariableGenerator varGen) {
        return PipeMakerUtils.adjacentStepPipe(AdjacentStep.both, AdjacentStep.both, bindings, replacements);
    }
}
