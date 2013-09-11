package com.lambdazen.pixy;

import java.util.List;
import java.util.Map;

/** This interface is implemented by classes that implement predefined relations like out/3 and label/2. */
public interface PipeMaker {
	/** Returns the signature as relationName + "/" + arity */
	public String getSignature();

	/** Given the bindings, this method returns a PixyPipe and adds any new replacements typically of the form x -> $x */
	public PixyPipe makePipe(List<PixyDatum> bindings, Map<String, PixyDatum> replacements, VariableGenerator varGen);
}
