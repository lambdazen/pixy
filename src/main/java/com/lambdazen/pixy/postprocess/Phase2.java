package com.lambdazen.pixy.postprocess;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.pipes.MergePipe;
import com.lambdazen.pixy.pipes.NamedOutputPipe;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class Phase2 implements PipeVisitor {
    Set<String> usedSteps;

    public Phase2(Set<String> usedSteps) {
        this.usedSteps = usedSteps;
    }

    @Override
    public void visit(PixyPipe pp) {
        if (pp instanceof NamedOutputPipe) {
            String outputStep = ((NamedOutputPipe) pp).getOutputNamedStep();

            if ((outputStep != null) && outputStep.startsWith("__pixy") && !usedSteps.contains(outputStep)) {
                // This is as(x).coalesce(x) -- the coalesce can be dropped
                ((NamedOutputPipe) pp).clearOutputNamedStep();
            }
        } else if (pp instanceof MergePipe) {
            MergePipe mp = (MergePipe) pp;

            Set<String> outputs = new HashSet<String>(mp.getNamedOutputSteps());

            outputs.removeAll(usedSteps);
            Iterator<String> outputIter = outputs.iterator();
            while (outputIter.hasNext()) {
                if (!(outputIter.next().startsWith("__pixy"))) {
                    outputIter.remove();
                }
            }

            mp.setUnusedNamedOutputStep(outputs);
        }
    }

    @Override
    public void reset() {
        // Nothing to do
    }

    @Override
    public void mark() {
        // Nothing to do
    }
}
