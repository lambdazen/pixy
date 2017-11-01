package com.lambdazen.pixy.pipes;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyDatum;
import com.lambdazen.pixy.PixyDatumType;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.gremlin.GraphTraversalExt;

public class FilterPipe implements PixyPipe, NamedInputPipe, InternalLookupPipe {
	private String namedStep;
	private PixyDatum value;

	public FilterPipe(String namedStep, PixyDatum value) {
		this.namedStep = namedStep;
		this.value = value;
		
		assert value.isFixed() || value.isAtomAPipeVar();
	}
	
	public String toString() {
 		return "pixyEval($" + ((namedStep == null) ? "" : namedStep) + " ifeq " + value + ")";
	}

	@Override
	public GraphTraversal pixyStep(GraphTraversal inputPipe) {
		GraphTraversal ans = inputPipe;
		
		PixyDatum[] ops = new PixyDatum[] {
				new PixyDatum(PixyDatumType.SPECIAL_ATOM, "$" + ((namedStep == null) ? "" : namedStep)),
				value,
				new PixyDatum(PixyDatumType.SPECIAL_ATOM, "ifeq/2")
		};

		ans = GraphTraversalExt.pixyEval(ans, Arrays.asList(ops));

		return ans;
	}

	@Override
	public String getInputNamedStep() {
		return namedStep;
	}

	@Override
	public void clearInputNamedStep() {
		this.namedStep = null;
	}

	@Override
	public void visit(PipeVisitor visitor) {
		visitor.visit(this);
	}
	
	@Override
	public Set<String> getDependentNamedSteps() {
		// The named step is already counted through the NamedInputPipe interface
		Set<String> ans = new HashSet<String>();

		if (value.isAtomAPipeVar()) {
			ans.add(value.getAtomVarName());
		}
		
		return ans;
	}
}
