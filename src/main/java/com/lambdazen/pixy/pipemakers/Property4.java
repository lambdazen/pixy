package com.lambdazen.pixy.pipemakers;

import com.lambdazen.pixy.PipeMaker;
import com.lambdazen.pixy.PixyDatum;
import com.lambdazen.pixy.PixyErrorCodes;
import com.lambdazen.pixy.PixyException;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.VariableGenerator;
import java.util.List;
import java.util.Map;

public class Property4 implements PipeMaker {
    @Override
    public String getSignature() {
        return "property/4";
    }

    @Override
    public PixyPipe makePipe(List<PixyDatum> bindings, Map<String, PixyDatum> replacements, VariableGenerator varGen) {
        PixyDatum element = bindings.get(0);
        PixyDatum key = bindings.get(1);
        PixyDatum value = bindings.get(2);
        PixyDatum defaultValue = bindings.get(3);

        Object defaultValueObj;
        switch (defaultValue.getType()) {
            case NUMBER:
                defaultValueObj = defaultValue.getNumber();
                break;

            case STRING:
                defaultValueObj = defaultValue.getString();
                break;

            case SPECIAL_ATOM:
                if (defaultValue.isTrue()) {
                    defaultValueObj = Boolean.TRUE;
                    break;
                } else if (defaultValue.isFail()) {
                    defaultValueObj = Boolean.FALSE;
                    break;
                }

            default:
                throw new PixyException(
                        PixyErrorCodes.EXPECTING_A_STRING_NUMBER_OR_BOOLEAN,
                        "Encountered a default value: " + defaultValue);
        }

        return Property3.makePropertyPipe(replacements, element, key, value, defaultValueObj);
    }
}
