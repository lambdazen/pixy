package com.lambdazen.pixy.pipemakers;

import java.util.List;
import java.util.Map;

import com.lambdazen.pixy.PipeMaker;
import com.lambdazen.pixy.PixyDatum;
import com.lambdazen.pixy.PixyDatumType;
import com.lambdazen.pixy.PixyErrorCodes;
import com.lambdazen.pixy.PixyException;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.VariableGenerator;
import com.lambdazen.pixy.pipes.ConnectPipe;
import com.lambdazen.pixy.pipes.FilterPipe;
import com.lambdazen.pixy.pipes.PropertyPipe;
import com.lambdazen.pixy.pipes.MatchPipe;

public class Property3 implements PipeMaker {
	@Override
	public String getSignature() {
		return "property/3";
	}
	
	@Override
	public PixyPipe makePipe(List<PixyDatum> bindings, Map<String, PixyDatum> replacements, VariableGenerator varGen) {
		PixyDatum element = bindings.get(0);
		PixyDatum key = bindings.get(1);
		PixyDatum value = bindings.get(2);
		
		return makePropertyPipe(replacements, element, key, value, null);
	}

	protected static PixyPipe makePropertyPipe(Map<String, PixyDatum> replacements,
			PixyDatum element, PixyDatum key, PixyDatum value, Object defaultValue) {
		if (key.getType() != PixyDatumType.STRING) {
			throw new PixyException(PixyErrorCodes.EXPECTING_A_STRING, "Encountered non-string property key: " + key);
		}
		
		if (element.isGround() && !value.isGround()) {
			if (element.isFixed()) {
				throw new PixyException(PixyErrorCodes.EXPECTING_A_PIPE, "Encountered a constant: " + element);
			}

			replacements.put(value.getVarName(), new PixyDatum(PixyDatumType.SPECIAL_ATOM, "$" + value.getVarName()));
			return new PropertyPipe(element.getAtomVarName(), key.getString(), value.getVarName(), defaultValue);
		} else if (element.isGround() && value.isGround()) {
			if (element.isFixed()) {
				throw new PixyException(PixyErrorCodes.EXPECTING_A_PIPE, "Encountered a constant: " + element);
			}

			if (value.isFixed()) {
				return new ConnectPipe(new PropertyPipe(element.getAtomVarName(), key.getString(), null, defaultValue),
						new FilterPipe(null, value));
			} else {
				return new ConnectPipe(new PropertyPipe(element.getAtomVarName(), key.getString(), null, defaultValue),
						new MatchPipe(value.getAtomVarName()));
			}
		} else {
			return null;
		}
	}
}
