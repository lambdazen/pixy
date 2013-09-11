package com.lambdazen.pixy.pipes;

import java.util.Arrays;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyErrorCodes;
import com.lambdazen.pixy.PixyException;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.gremlin.GremlinPipelineExt;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe.LoopBundle;

/** This pipe implements a wrapper for in, out, both, inE, outE, bothE, inV, outV and bothV */
public class AdjacentStepPipe implements PixyPipe, NamedInputPipe, NamedOutputPipe {
	private AdjacentStep step;
	private String inStep;
	private String[] labels;
	private String outStep;
	private LoopDetails loopDetails;

	public AdjacentStepPipe(AdjacentStep step, String namedStep, String[] labels, String varName) {
		this.step = step;
		this.inStep = namedStep;
		this.labels = labels;
		this.outStep = varName;
	}
	
	public String toString() {
		String loopAsStep = (loopDetails == null) ? "" : ("as(" + loopDetails.getLoopVarName() + ") -> ");
		String loopLoopStep = (loopDetails == null) ? "" : (" -> loop(" + loopDetails.getLoopVarName() + ")");

		return ((inStep == null) ? "" : "coalesce('" + inStep + "') -> ")
				+ loopAsStep		
				+ step.toString() + "(" + (labels == null ? "ALL" : Arrays.asList(labels)) + ")"
				+ loopLoopStep		
				+ ((outStep == null) ? "" : " -> as('" + outStep + "')");
	}
	
	@Override
	public GremlinPipeline pixyStep(GremlinPipeline inputPipe) {
		GremlinPipeline ans = inputPipe;

		if (inStep != null) {
			ans = GremlinPipelineExt.coalesce(ans, inStep);
		}

		if (loopDetails != null) {
			ans = ans.as(loopDetails.getLoopVarName());
		}

		if (labels == null) {
			switch (step) {
			case in:
				ans = ans.in();
				break;

			case out:
				ans = ans.out();
				break;

			case both:
				ans = ans.both();
				break;

			case inV:
				ans = ans.inV();
				break;

			case outV:
				ans = ans.outV();
				break;
				
			case bothV:
				ans = ans.bothV();
				break;
				
			case inE:
				ans = ans.inE();
				break;
				
			case outE:
				ans = ans.outE();
				break;
				
			case bothE:
				ans = ans.bothE();
				break;
				
			default:
				throw new PixyException(PixyErrorCodes.INTERNAL_ERROR, "Unhandled adjacent step: " + step);
			}
		} else {
			switch (step) {
			case in:
				ans = ans.in(labels);
				break;
				
			case out:
				ans = ans.out(labels);
				break;
				
			case both:
				ans = ans.both(labels);
				break;

			case inE:
				ans = ans.inE(labels);
				break;
				
			case outE:
				ans = ans.outE(labels);
				break;
				
			case bothE:
				ans = ans.bothE(labels);
				break;

			default:
				throw new PixyException(PixyErrorCodes.INTERNAL_ERROR, "Unhandled adjacent step: " + step);
			}
		}

		if (loopDetails != null) {
			ans = ans.loop(loopDetails.getLoopVarName(), new PipeFunction<LoopBundle, Boolean>() {
				@Override
				public Boolean compute(LoopBundle lb) { // while closure
					int length = lb.getLoops() - 1;
					return ((loopDetails.getMaxLoops() == 0) || length < loopDetails.getMaxLoops());
				}
			}, new PipeFunction<LoopBundle, Boolean>() { // emit closure
				@Override
				public Boolean compute(LoopBundle lb) {
					int length = lb.getLoops() - 1;
					return length >= loopDetails.getMinLoops();
				}
			});
		}

		if (outStep != null) {
			ans = ans.as(outStep);
		}
		
		return ans;
	}

	@Override
	public String getInputNamedStep() {
		return inStep;
	}

	@Override
	public void clearInputNamedStep() {
		this.inStep = null;
	}

	@Override
	public String getOutputNamedStep() {
		return outStep;
	}

	@Override
	public void clearOutputNamedStep() {
		this.outStep = null;
	}

	@Override
	public void visit(PipeVisitor visitor) {
		visitor.visit(this);
	}
	
	public void setLoopDetails(LoopDetails ld) {
		this.loopDetails = ld;
	}
}
