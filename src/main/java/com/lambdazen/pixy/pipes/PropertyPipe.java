package com.lambdazen.pixy.pipes;

import com.lambdazen.pixy.PipeVisitor;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.gremlin.GremlinPipelineExt;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;

public class PropertyPipe implements PixyPipe, NamedInputPipe, NamedOutputPipe {
	private String inStep;
	private String key;
	private String outStep;
	private Object defaultValue;

	public PropertyPipe(String namedStep, String key, String varName, Object defaultValue) {
		this.inStep = namedStep;
		this.key = key;
		this.outStep = varName;
		this.defaultValue = defaultValue;
	}

	public String toString() {
		return ((inStep == null) ? "" : "coalesce('" + inStep + "') -> ") 
				+ "property('" + key + "')"
				+ (defaultValue == null ? " -> filter(this != null)" : " -> transform(this == null ? " + defaultValue + " : this)")
				+ ((outStep == null) ? "" : " -> as('" + outStep + "')");
	}

	@Override
	public GremlinPipeline pixyStep(GremlinPipeline inputPipe) {
		GremlinPipeline ans = inputPipe;

		if (inStep != null) {
			ans = GremlinPipelineExt.coalesce(ans, inStep);
		}
		
		if (defaultValue == null) {
			// Without default
			ans = ans.property(key).filter(new PipeFunction() {
				@Override
				public Boolean compute(Object x) {
					return (x != null);
				}
				
			});
		} else {
			// With default
			ans = ans.property(key).transform(new PipeFunction() {
				@Override
				public Object compute(Object x) {
					return (x == null) ? defaultValue : x;
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
}
