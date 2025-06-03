package com.lambdazen.pixy.pipemakers;

import com.lambdazen.pixy.PixyDatum;
import com.lambdazen.pixy.PixyDatumType;
import com.lambdazen.pixy.PixyErrorCodes;
import com.lambdazen.pixy.PixyException;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.VariableGenerator;
import com.lambdazen.pixy.pipes.AdjacentStep;
import com.lambdazen.pixy.pipes.AdjacentStepPipe;
import com.lambdazen.pixy.pipes.ConnectPipe;
import com.lambdazen.pixy.pipes.LoopDetails;
import com.lambdazen.pixy.pipes.MatchPipe;
import com.lambdazen.pixy.postprocess.LoopSetter;
import java.util.List;
import java.util.Map;

public class PipeMakerUtils {
    public static String[] getLabelsFromDatum(PixyDatum labelDatum) {
        if (labelDatum.getType() == PixyDatumType.STRING) {
            return new String[] {labelDatum.getString()};
        } else if (labelDatum.getType() == PixyDatumType.LIST) {
            List<PixyDatum> labelList = labelDatum.getList();
            String[] ans = new String[labelList.size()];

            for (int i = 0; i < ans.length; i++) {
                PixyDatum labelListItem = labelList.get(i);
                if (labelListItem.getType() != PixyDatumType.STRING) {
                    throw new PixyException(
                            PixyErrorCodes.EXPECTING_A_STRING_OR_LIST_OF_STRINGS,
                            "Encountered non-string value '" + labelListItem + "' in " + labelDatum);
                }
                ans[i] = labelList.get(i).getString();
            }
            return ans;
        } else {
            throw new PixyException(PixyErrorCodes.EXPECTING_A_STRING_OR_LIST_OF_STRINGS, "Encountered " + labelDatum);
        }
    }

    public static int[] getLoopLimitsFromDatum(PixyDatum labelDatum) {
        if (labelDatum.getType() == PixyDatumType.NUMBER) {
            return new int[] {0, labelDatum.getNumber().intValue()};
        } else if (labelDatum.getType() == PixyDatumType.LIST) {
            List<PixyDatum> labelList = labelDatum.getList();

            if (labelList.size() != 2) {
                throw new PixyException(
                        PixyErrorCodes.EXPECTING_A_NUMBER_OR_LIST_OF_TWO_NUMBERS,
                        "Encountered a list of size '" + labelList.size() + "' in " + labelDatum);
            }

            int[] ans = new int[2];

            for (int i = 0; i < 2; i++) {
                PixyDatum labelListItem = labelList.get(i);
                if (labelListItem.getType() != PixyDatumType.NUMBER) {
                    throw new PixyException(
                            PixyErrorCodes.EXPECTING_A_NUMBER_OR_LIST_OF_TWO_NUMBERS,
                            "Encountered non-string value '" + labelListItem + "' at index " + i + "in " + labelDatum);
                }

                ans[i] = labelList.get(i).getNumber().intValue();
            }
            return ans;
        } else {
            throw new PixyException(PixyErrorCodes.EXPECTING_A_STRING_OR_LIST_OF_STRINGS, "Encountered " + labelDatum);
        }
    }

    /** Returns a pipe for a predicate that is an adjacent step, like outV or inE */
    public static PixyPipe adjacentStepPipe(
            AdjacentStep forwardStep,
            AdjacentStep reverseStep,
            List<PixyDatum> bindings,
            Map<String, PixyDatum> replacements) {
        PixyDatum start = bindings.get(0);
        String[] labels;
        PixyDatum end;

        if (bindings.size() == 2) {
            labels = null;
            end = bindings.get(1);
        } else if (bindings.size() == 3) {
            labels = PipeMakerUtils.getLabelsFromDatum(bindings.get(1));
            end = bindings.get(2);
        } else {
            throw new PixyException(
                    PixyErrorCodes.INTERNAL_ERROR,
                    "Unsupported bindings arity. Forward step " + forwardStep + ". Bindings: " + bindings);
        }

        if (start.isGround() && !end.isGround()) {
            if (start.isFixed()) {
                throw new PixyException(PixyErrorCodes.EXPECTING_A_PIPE, "Encountered a constant: " + start);
            }

            replacements.put(end.getVarName(), new PixyDatum(PixyDatumType.SPECIAL_ATOM, "$" + end.getVarName()));

            return new AdjacentStepPipe(forwardStep, start.getAtomVarName(), labels, end.getVarName());
        } else if (!start.isGround() && end.isGround()) {
            if (end.isFixed()) {
                throw new PixyException(PixyErrorCodes.EXPECTING_A_PIPE, "Encountered a constant: " + end);
            }

            replacements.put(start.getVarName(), new PixyDatum(PixyDatumType.SPECIAL_ATOM, "$" + start.getVarName()));
            return new AdjacentStepPipe(reverseStep, end.getAtomVarName(), labels, start.getVarName());
        } else if (start.isGround() && end.isGround()) {
            if (end.isFixed()) {
                throw new PixyException(PixyErrorCodes.EXPECTING_A_PIPE, "Encountered a constant: " + end);
            }

            if (start.isFixed()) {
                throw new PixyException(PixyErrorCodes.EXPECTING_A_PIPE, "Encountered a constant: " + start);
            }

            return new ConnectPipe(
                    new AdjacentStepPipe(forwardStep, start.getAtomVarName(), labels, null),
                    new MatchPipe(end.getAtomVarName()));
        } else {
            return null;
        }
    }

    /** Returns a pipe for inLoop, outLoop and bothLoop */
    public static PixyPipe adjacentLoopPipe(
            AdjacentStep forwardStep,
            AdjacentStep reverseStep,
            List<PixyDatum> bindings,
            Map<String, PixyDatum> replacements,
            VariableGenerator varGen) {
        String loopVarName = varGen.newVariable("loop");

        if (bindings.size() == 2) {
            PixyPipe step = adjacentStepPipe(forwardStep, reverseStep, bindings, replacements);

            step.visit(new LoopSetter(new LoopDetails(loopVarName, 0, 0)));

            return step;
        } else if (bindings.size() == 4) {
            List<PixyDatum> adjBindings = bindings.subList(0, 3);
            PixyDatum counts = bindings.get(3);

            PixyPipe step = adjacentStepPipe(forwardStep, reverseStep, adjBindings, replacements);
            int[] loopLimits = getLoopLimitsFromDatum(counts);

            step.visit(new LoopSetter(new LoopDetails(loopVarName, loopLimits[0], loopLimits[1])));

            return step;
        } else if (bindings.size() == 3) {
            // First try inLoop(Vertex, Vertex, Loop Limits)
            int[] loopLimits;
            List<PixyDatum> adjBindings;

            try {
                PixyDatum counts = bindings.get(2);
                loopLimits = PipeMakerUtils.getLoopLimitsFromDatum(counts);
                adjBindings = bindings.subList(0, 2);
            } catch (PixyException e) {
                // Failed. Now try inLoop(Vertex, Labels, Vertex)
                adjBindings = bindings.subList(0, 3);
                loopLimits = new int[] {0, 0};
            }

            PixyPipe step = adjacentStepPipe(forwardStep, reverseStep, adjBindings, replacements);

            step.visit(new LoopSetter(new LoopDetails(loopVarName, loopLimits[0], loopLimits[1])));

            return step;
        } else {
            throw new PixyException(
                    PixyErrorCodes.INTERNAL_ERROR,
                    "Unsupported bindings arity. Forward step " + forwardStep + ". Bindings: " + bindings);
        }
    }
}
