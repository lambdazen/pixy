package com.lambdazen.pixy;

/** This class generates a unique variable name given a variable name */
public class VariableGenerator {
    private int counter;
    private int markCounter;

    public VariableGenerator() {
        this.counter = 1;
    }

    public String newVariable(String varName) {
        if (varName.startsWith("__pixy")) {
            // Remove trailing number
            varName = varName.replaceAll("_[0-9]+$", "");
            return varName + "_" + (counter++);
        } else {
            return "__pixy_" + varName + "_" + (counter++);
        }
    }

    public void mark() {
        markCounter = counter;
    }

    public void reset() {
        counter = markCounter;
    }
}
