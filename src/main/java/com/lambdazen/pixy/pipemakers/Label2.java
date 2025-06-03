package com.lambdazen.pixy.pipemakers;

import com.lambdazen.pixy.PipeMaker;
import com.lambdazen.pixy.PixyDatum;
import com.lambdazen.pixy.PixyDatumType;
import com.lambdazen.pixy.PixyErrorCodes;
import com.lambdazen.pixy.PixyException;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.VariableGenerator;
import com.lambdazen.pixy.pipes.ConnectPipe;
import com.lambdazen.pixy.pipes.FilterPipe;
import com.lambdazen.pixy.pipes.LabelPipe;
import java.util.List;
import java.util.Map;

public class Label2 implements PipeMaker {
    @Override
    public String getSignature() {
        return "label/2";
    }

    @Override
    public PixyPipe makePipe(List<PixyDatum> bindings, Map<String, PixyDatum> replacements, VariableGenerator varGen) {
        PixyDatum element = bindings.get(0);
        PixyDatum value = bindings.get(1);

        if (element.isGround() && !value.isGround()) {
            if (element.isFixed()) {
                throw new PixyException(PixyErrorCodes.EXPECTING_A_PIPE, "Encountered a constant: " + element);
            }

            replacements.put(value.getVarName(), new PixyDatum(PixyDatumType.SPECIAL_ATOM, "$" + value.getVarName()));
            return new LabelPipe(element.getAtomVarName(), value.getVarName());
        } else if (element.isGround() && value.isGround()) {
            if (element.isFixed()) {
                throw new PixyException(PixyErrorCodes.EXPECTING_A_PIPE, "Encountered a constant: " + element);
            }

            return new ConnectPipe(
                    new LabelPipe(element.getAtomVarName(), value.getVarName()), new FilterPipe(null, value));
        } else {
            return null;
        }
    }
}
