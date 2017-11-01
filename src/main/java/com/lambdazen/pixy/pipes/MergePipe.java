package com.lambdazen.pixy.pipes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.gremlin.GraphTraversalExt;

public class MergePipe implements PixyPipe, InternalLookupPipe {	
	List<PixyPipe> orPipes;
	Map<String, List<String>> mergeVarsMap;
	String lastNamedOutputStep;
	Set<String> unusedNamedOutputSteps;

	public MergePipe(List<PixyPipe> orPipes, Map<String, List<String>> mergeVarsMap) {
		this.orPipes = orPipes;
		this.mergeVarsMap = mergeVarsMap;
		this.unusedNamedOutputSteps = new HashSet<String>();
	}

	public String toString() {
		StringBuffer ans = new StringBuffer("merge(" + orPipes + ")");
		for (String key : mergeVarsMap.keySet()) {
			if ((lastNamedOutputStep == null) || !(key.equals(lastNamedOutputStep))) {
				ans.append(coalesceStr(key));
			}
		}
		
		// Emit the last named step
		if (lastNamedOutputStep != null) {
			ans.append(coalesceStr(lastNamedOutputStep));
		}
		
		return ans.toString();
	}
	
	private String coalesceStr(String namedStep) {
		List<String> mergeVars = new ArrayList<String>(mergeVarsMap.get(namedStep));
		Collections.reverse(mergeVars);
		
		if (unusedNamedOutputSteps.contains(namedStep)) {
			return " -> coalesce(" + mergeVars + ")";
		} else {
			return " -> coalesce(" + mergeVars + ") -> as('" + namedStep + "')";
		}
	}

	@Override
	public GraphTraversal pixyStep(GraphTraversal inputPipe) {
		GraphTraversal ans = inputPipe;
		
		List<GraphTraversal> pipesToMerge = new ArrayList<GraphTraversal>();
		
		for (PixyPipe pixyPipe : orPipes) {
			GraphTraversal pipeline = pixyPipe.pixyStep(__());
			pipesToMerge.add(pipeline);
		}
		
		ans = GraphTraversalExt.pixySplitMerge(ans, pipesToMerge);
		
		for (String namedStep : mergeVarsMap.keySet()) {
			if ((lastNamedOutputStep == null) || !(namedStep.equals(lastNamedOutputStep))) {
				pixyCoalesce(ans, namedStep);
			}
		}
		
		// Emit the last named step
		if (lastNamedOutputStep != null) {
			pixyCoalesce(ans, lastNamedOutputStep);
		}

		return ans;
	}

	private GraphTraversal pixyCoalesce(GraphTraversal pipeline, String namedStep) {
		List<String> mergeVars = new ArrayList<String>(mergeVarsMap.get(namedStep));
		Collections.reverse(mergeVars);
		
		if (unusedNamedOutputSteps.contains(namedStep)) {
			if (!namedStep.equals(lastNamedOutputStep)) {
				// Nothing to do
				return pipeline;
			} else {
				// Just the coalesce
				return GraphTraversalExt.coalesce(pipeline, mergeVars.toArray(new String[0]));
			}
		} else {
			// Coalesce and as
			return GraphTraversalExt.coalesce(pipeline, mergeVars.toArray(new String[0])).as(namedStep);
		}
	}

	@Override
	public Set<String> getDependentNamedSteps() {
		Set<String> ans = new HashSet<String>();
		
		for (PixyPipe pixyPipe : orPipes) {
			PipeUtils.addDependentNamedSteps(ans, pixyPipe);
		}

		for (Map.Entry<String, List<String>> entry : mergeVarsMap.entrySet()) {
			List<String> mergeVars = new ArrayList<String>(entry.getValue());
			ans.addAll(mergeVars);
		}

		return ans;
	}

	@Override
	public void visit(PipeVisitor visitor) {
		visitor.mark();

		for (PixyPipe pixyPipe : orPipes) {
			visitor.reset();
			
			pixyPipe.visit(visitor);
		}

		visitor.visit(this);
	}
	
	public void setLastNamedOuput(String namedStep) {
		this.lastNamedOutputStep = namedStep;
	}

	public Set<String> getNamedOutputSteps() {
		return mergeVarsMap.keySet();
	}
	
	public void setUnusedNamedOutputStep(Set<String> steps) {
		unusedNamedOutputSteps = steps;
	}
}
