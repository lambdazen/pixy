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
import com.lambdazen.pixy.pipes.MatchPipe;

public class Is2 implements PipeMaker {
	@Override
	public String getSignature() {
		return "(is)/2";
	}

	@Override
	public PixyPipe makePipe(List<PixyDatum> bindings, Map<String, PixyDatum> replacements, VariableGenerator varGen) {
		// Eval pipe, then as() if var, match() if atom-var
		
		// The bindings have a variable and a list
		assert bindings.size() == 2;
		assert bindings.get(1).getType() == PixyDatumType.LIST;
		
		List<PixyDatum> list = bindings.get(1).getList();

		// Make sure the parameters are all bound
		for (PixyDatum item : list) {
			if (!item.isGround()) {
				return null;
			}
		}
		
		PixyDatum target = bindings.get(0);
		if (target.getType() == PixyDatumType.VARIABLE) {
			replacements.put(target.getVarName(), new PixyDatum(PixyDatumType.SPECIAL_ATOM, "$" + target.getVarName()));

			return new EvalPipe(target.getVarName(), list);
		} else if (target.isAtomAPipeVar()) {
			return new ConnectPipe(new EvalPipe(null, list), new MatchPipe(target.getAtomVarName()));
		} else {
			return new ConnectPipe(new EvalPipe(null, list), new FilterPipe(null, target));
		}
	}
}
