package com.lambdazen.pixy;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PixyQueryParser {
    public static final String PIXY_OUTPUT = "__pixy_output";
    public static final String PIXY_INPUT = "__pixy_input";

    public enum FieldType {
        INPUT_STEP,
        OUTPUT_STEP,
        PARAMETER
    };

    public static final Pattern QUERY_REGEX = Pattern.compile("\\s*([^() ]+)[(]([^)]*)[)]\\s*");
    private String relName;
    private String[] fieldNames;
    private FieldType[] fieldTypes;
    private int numParams = 0;
    private boolean hasDefinedOutput = false;
    private boolean hasDefinedInput = false;
    private int counter = 1;

    public PixyQueryParser(String s) {
        if (!s.matches(QUERY_REGEX.pattern())) {
            throw new PixyException(PixyErrorCodes.INVALID_QUERY_FORM, "Encountered: " + s);
        }

        Matcher m = QUERY_REGEX.matcher(s);
        boolean found = m.find();

        assert found;

        this.relName = m.group(1);

        String[] paramArr = m.group(2).split(",");
        this.fieldNames = new String[paramArr.length];
        this.fieldTypes = new FieldType[paramArr.length];
        for (int i = 0; i < paramArr.length; i++) {
            String param = paramArr[i].trim();

            if (param.equals("?")) {
                // Parameter
                fieldNames[i] = null;
                fieldTypes[i] = FieldType.PARAMETER;
                numParams++;
            } else if (param.equals("_")) {
                // Output that doesn't matter. Named with __pixy so that it can be removed if not needed within the pipe
                fieldNames[i] = "__pixy_ignore_" + (counter++);
                fieldTypes[i] = FieldType.OUTPUT_STEP;
            } else if (param.startsWith("&")) {
                fieldNames[i] = param.substring(1);
                fieldTypes[i] = FieldType.OUTPUT_STEP;

                if (fieldNames[i].equals("")) {
                    fieldNames[i] = PIXY_OUTPUT;
                    this.hasDefinedOutput = true;
                }
            } else if (param.startsWith("$")) {
                fieldNames[i] = param.substring(1);
                fieldTypes[i] = FieldType.INPUT_STEP;

                if (fieldNames[i].equals("")) {
                    fieldNames[i] = PIXY_INPUT;
                    this.hasDefinedInput = true;
                }
            } else {
                throw new PixyException(
                        PixyErrorCodes.INVALID_QUERY_PAREMETER, "Encountered " + param + " in query " + s);
            }
        }
    }

    public String getRelName() {
        return relName;
    }

    public List<PixyDatum> getBindings(Object[] params) {
        if (params.length != numParams) {
            throw new PixyException(
                    PixyErrorCodes.QUERY_PLACEHOLDER_MISMATCH,
                    "Expecting " + numParams + " parameters, but got " + params.length + " parameters");
        }

        List<PixyDatum> bindings = new ArrayList<PixyDatum>();
        int paramCounter = 0;

        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = fieldNames[i];
            FieldType fieldType = fieldTypes[i];

            switch (fieldType) {
                case INPUT_STEP:
                    bindings.add(new PixyDatum(PixyDatumType.SPECIAL_ATOM, "$" + fieldName));
                    break;

                case OUTPUT_STEP:
                    bindings.add(new PixyDatum(PixyDatumType.VARIABLE, fieldName));
                    break;

                case PARAMETER:
                    Object param = params[paramCounter++];
                    if (PixyDatum.isNumber(param)) {
                        bindings.add(new PixyDatum(PixyDatumType.NUMBER, PixyDatum.convertToBigDecimal(param)));
                    } else {
                        bindings.add(new PixyDatum(PixyDatumType.STRING, param.toString()));
                    }
                    break;

                default:
                    throw new PixyException(PixyErrorCodes.INTERNAL_ERROR, "Unhandled type " + fieldType);
            }
        }

        return bindings;
    }

    public boolean hasDefinedOutput() {
        return hasDefinedOutput;
    }

    public boolean hasDefinedInput() {
        return hasDefinedInput;
    }
}
