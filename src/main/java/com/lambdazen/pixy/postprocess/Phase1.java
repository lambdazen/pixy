package com.lambdazen.pixy.postprocess;

import java.util.Set;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.pipes.MergePipe;
import com.lambdazen.pixy.pipes.NamedInputPipe;
import com.lambdazen.pixy.pipes.NamedOutputPipe;
import com.lambdazen.pixy.pipes.PipeUtils;

public class Phase1 implements PipeVisitor {
	private String curOutputStep;
	private Set<String> usedSteps;
	private MergePipe prevMergePipe;
	private String markOutputStep;
	
	public Phase1(Set<String> usedSteps) {
		this.usedSteps = usedSteps;
		this.curOutputStep = null;
	}

	@Override
	public void visit(PixyPipe pp) {
		if (pp instanceof NamedInputPipe) {
			String inputStep = ((NamedInputPipe)pp).getInputNamedStep();

			if (inputStep != null) {
				if ((curOutputStep != null) && (curOutputStep.equals(inputStep))) {
					// This is as(x).coalesce(x) -- the coalesce can be dropped
					((NamedInputPipe)pp).clearInputNamedStep();
				}

				if ((prevMergePipe != null) && prevMergePipe.getNamedOutputSteps().contains(inputStep)) {
					// This is a merge[ ].coalesce().as() . coalesce()
					((NamedInputPipe)pp).clearInputNamedStep();
					
					// Make sure the coalesce().as() is the last step
					prevMergePipe.setLastNamedOuput(inputStep);
				}
			}
		}

		PipeUtils.addDependentNamedSteps(usedSteps, pp);
		
		if (pp instanceof NamedOutputPipe) {
			this.curOutputStep = ((NamedOutputPipe)pp).getOutputNamedStep();
			this.prevMergePipe = null;
		} else if (pp instanceof MergePipe) {
			// Merge pipes are flexible in what the final output can be
			this.curOutputStep = null;
			this.prevMergePipe = (MergePipe)pp;
		} else {
			this.curOutputStep = null;
			this.prevMergePipe = null;
		}
		
	}

	@Override
	public void reset() {
		this.curOutputStep = markOutputStep;
	}

	@Override
	public void mark() {
		this.markOutputStep = curOutputStep;
	}
}
