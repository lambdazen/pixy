package com.lambdazen.pixy.pipemakers;

import com.lambdazen.pixy.PipeMaker;
import com.lambdazen.pixy.PixyDatum;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.VariableGenerator;
import com.lambdazen.pixy.pipes.AdjacentStep;
import java.util.List;
import java.util.Map;

public class InLoop3 implements PipeMaker {
    @Override
    public String getSignature() {
        return "inLoop/3";
    }

    @Override
    public PixyPipe makePipe(List<PixyDatum> bindings, Map<String, PixyDatum> replacements, VariableGenerator varGen) {
        return PipeMakerUtils.adjacentLoopPipe(AdjacentStep.in, AdjacentStep.out, bindings, replacements, varGen);
    }
}
