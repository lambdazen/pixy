package com.lambdazen.pixy.pipemakers;

import java.util.List;
import java.util.Map;

import com.lambdazen.pixy.PipeMaker;
import com.lambdazen.pixy.PixyDatum;
import com.lambdazen.pixy.PixyDatumType;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.VariableGenerator;

import com.lambdazen.pixy.pipes.ConnectPipe;
import com.lambdazen.pixy.pipes.EvalPipe;
import com.lambdazen.pixy.pipes.FilterPipe;

public class Bool1 implements PipeMaker {
	@Override
	public String getSignature() {
		return "(bool)/1";
	}

	@Override
	public PixyPipe makePipe(List<PixyDatum> bindings, Map<String, PixyDatum> replacements, VariableGenerator varGen) {
		// Eval pipe, then as() if var, match() if atom-var
		
		// The bindings have a variable and a list
		assert bindings.size() == 1;
		assert bindings.get(0).getType() == PixyDatumType.LIST;
		
		List<PixyDatum> list = bindings.get(0).getList();

		// Make sure the parameters are all bound
		for (PixyDatum item : list) {
			if (!item.isGround()) {
				return null;
			}
		}
		
		return new ConnectPipe(new EvalPipe(null, list), new FilterPipe(null, new PixyDatum(PixyDatumType.SPECIAL_ATOM, "true")));
	}
}
