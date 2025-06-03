package com.lambdazen.pixy;

import com.lambdazen.pixy.gremlin.PixySplitMergeStep;
import com.lambdazen.pixy.pipes.*;
import com.lambdazen.pixy.postprocess.Phase1;
import com.lambdazen.pixy.postprocess.Phase2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a relation and some 'ground' aka bound items, this class finds a path
 * to 'ground' other items using the rules. In other words, it performs static
 * analysis of the rules to see how it can be translated to pipes.
 */
public class PixyGrinder {
    private static final Logger log = LoggerFactory.getLogger(PixyTheory.class);

    PixyTheory theory;
    Stack<String> relationStack;

    public PixyGrinder(PixyTheory theory) {
        this.theory = theory;

        this.relationStack = new Stack<String>();
    }

    public PixyPipe convertQueryToPipe(String query, Object... params) {
        PixyQueryParser parser = new PixyQueryParser(query);

        PixyPipe pp = convertRuleToPixyPipe(parser.getRelName(), parser.getBindings(params));

        if (parser.hasDefinedInput()) {
            pp = new ConnectPipe(new SaveInputPipe(PixyQueryParser.PIXY_INPUT), pp);
        }

        if (parser.hasDefinedOutput()) {
            pp = new ConnectPipe(pp, new LoadOutputPipe(PixyQueryParser.PIXY_OUTPUT));
        }

        log.trace("Before post-processing: {}", pp);

        // Visitor 1: Store the previous output named step, get the input named steps, clear if same. Gather dependent
        // named steps.
        Set<String> usedSteps = new HashSet<String>();
        pp.visit(new Phase1(usedSteps));

        // Visitor 2: Get the output named step. Clear if starts with __pixy and is not in dependent named steps
        pp.visit(new Phase2(usedSteps));

        log.debug("Query {} was translated to pipe {}", query, pp);

        return pp;
    }

    public PixyPipe convertRuleToPixyPipe(String relName, List<PixyDatum> bindings) {
        PixyPipe ans =
                convertRuleToPixyPipe(relName, bindings, new VariableGenerator(), new HashMap<String, PixyDatum>());

        if (ans == null) {
            throw new PixyException(
                    PixyErrorCodes.SEQUENCING_ERROR, "Encountered rule " + relName + " with bindings " + bindings);
        } else {
            return ans;
        }
    }

    private PixyPipe convertRuleToPixyPipe(
            String relName, List<PixyDatum> bindings, VariableGenerator varGen, Map<String, PixyDatum> replacements) {
        // Get signature
        int arity = bindings.size();
        String signature = relName + "/" + arity;

        // Check for cycles
        if (relationStack.contains(signature)) {
            throw new PixyException(
                    PixyErrorCodes.RECURSION_NOT_SUPPORTED,
                    "Recursion detected at " + signature + ". The evaluation stack has " + relationStack.toString());
        }

        relationStack.push(signature);

        try {
            // Is this a not rule
            if (relName.startsWith("not(") && relName.endsWith(")")) {
                // This is a not(something(with, params))
                String rootRelName = relName.substring(4, relName.length() - 1);

                // The names of the variables in the relation tuple don't matter as long as they match the arity
                List<PixyDatum> relTuple = new ArrayList<PixyDatum>();
                for (int i = 0; i < arity; i++) {
                    relTuple.add(new PixyDatum(PixyDatumType.VARIABLE, "X_" + i));
                }

                // Not body 1 is of the form: something(with, params), !, fail.
                List<PixyDatum> notBody1 = Arrays.asList(new PixyDatum[] {
                    new PixyDatum(PixyDatumType.RELATION, rootRelName, relTuple), // something(with, params)
                    new PixyDatum(PixyDatumType.SPECIAL_ATOM, PixyDatum.CUT), // !
                    new PixyDatum(PixyDatumType.SPECIAL_ATOM, PixyDatum.FAIL) // fail
                });

                // Not clause looks like: __not_something(X_0, X_1) :- something(X_0, X_1), !, fail.
                // __not_something(X_0, X_1).
                // The bindings are (with, params)
                PixyClause notClause1 = new PixyClause(
                        new PixyDatum(PixyDatumType.RELATION, "__not_" + rootRelName, relTuple), notBody1);
                PixyClause notClause2 = new PixyClause(
                        new PixyDatum(PixyDatumType.RELATION, "__not_" + rootRelName, relTuple),
                        Collections.<PixyDatum>emptyList());
                List<PixyClause> notClauses = Arrays.asList(new PixyClause[] {notClause1, notClause2});

                return convertOrClausesToPixyPipe(notClauses, bindings, varGen, replacements);
            }

            // Predefined rule
            PipeMaker pipeMaker = theory.getPredefinedRules().get(signature);
            if (pipeMaker != null) {
                return pipeMaker.makePipe(bindings, replacements, varGen);
            }

            // User-defined rule
            List<PixyClause> clauses = theory.getClauses(signature);
            if (clauses == null) {
                throw new PixyException(
                        PixyErrorCodes.REFERENCE_TO_MISSING_RULE,
                        "Encountered reference to '" + signature + "'. Evaluation stack: " + relationStack);
            }

            return convertOrClausesToPixyPipe(clauses, bindings, varGen, replacements);
        } finally {
            relationStack.pop();
        }
    }

    protected PixyPipe convertOrClausesToPixyPipe(
            List<PixyClause> clauses,
            List<PixyDatum> bindings,
            VariableGenerator varGen,
            Map<String, PixyDatum> replacements) {
        // Go over the pipes
        boolean mergeResultingBindings = (clauses.size() > 1);

        // Using a tree map to preserve the order
        Map<String, List<String>> mergeVarsMap = new TreeMap<String, List<String>>();

        List<PixyPipe> orPipes = new ArrayList<PixyPipe>();
        boolean someInvalidPipes = false;
        for (PixyClause clause : clauses) {
            Map<String, PixyDatum> newReplacements = new HashMap<String, PixyDatum>();
            newReplacements.putAll(replacements);

            List<PixyDatum> newBindings = bindings;
            newBindings =
                    renameOutputsIfNecessary(mergeResultingBindings, bindings, mergeVarsMap, newReplacements, varGen);

            PixyPipe clausePipe = convertClauseToPipe(clause, newBindings, varGen, newReplacements);
            if (clausePipe != null) {
                orPipes.add(clausePipe);
            } else {
                someInvalidPipes = true;
            }
        }

        // All the outputs should be bound now
        for (String varName : mergeVarsMap.keySet()) {
            replacements.put(varName, new PixyDatum(PixyDatumType.SPECIAL_ATOM, "$" + varName));
        }

        if (someInvalidPipes || (orPipes.size() == 0)) {
            return null;
        } else if (orPipes.size() == 1) {
            return orPipes.get(0);
        } else {
            return new MergePipe(orPipes, mergeVarsMap);
        }
    }

    private List<PixyDatum> renameOutputsIfNecessary(
            boolean rename,
            List<PixyDatum> bindings,
            Map<String, List<String>> mergeVarsMap,
            Map<String, PixyDatum> replacements,
            VariableGenerator varGen) {
        List<PixyDatum> ans = new ArrayList<PixyDatum>();

        for (PixyDatum binding : bindings) {
            if (binding.getType() != PixyDatumType.VARIABLE) {
                // This is an input, not an output -- OK to leave as is
                ans.add(binding);
            } else {
                String varName = binding.getVarName();
                String newVarName = varGen.newVariable(varName);
                PixyDatum newBinding = (!rename) ? binding : new PixyDatum(PixyDatumType.VARIABLE, newVarName);

                // Change the binding to use a new name
                ans.add(newBinding);

                // Make sure the replacement is tracked
                if (rename) {
                    replacements.put(varName, newBinding);
                }

                // Keep track of what to merge
                if (!rename) {
                    mergeVarsMap.put(varName, null);
                } else {
                    List<String> mergeVars = mergeVarsMap.get(varName);
                    if (mergeVars == null) {
                        mergeVars = new ArrayList<String>();
                        mergeVarsMap.put(varName, mergeVars);
                    }

                    mergeVars.add(newVarName);
                }
            }
        }

        return ans;
    }

    private PixyPipe convertClauseToPipe(
            PixyClause clause,
            List<PixyDatum> bindings,
            VariableGenerator varGen,
            Map<String, PixyDatum> replacements) {
        // Rename the clause to have unique variable names
        clause = clause.renameToUniqueVars(varGen);

        // Find the head
        PixyDatum head = clause.getHead();

        assert (head.getType() == PixyDatumType.RELATION);

        // First unify the target to the bindings
        // Substitute all VARs what is provided
        // The pipe maker will return additional bindings of the form X ->
        // $temp_1, which will also be subst. It will also return who is $ (last var).
        // Each pipe will also maintain what is needed.
        // Post-process to remove .as('non-needed').
        List<PixyDatum> target = head.getRelationTuple();

        // Unify the bindings to the target. This may or may not return a pipe
        log.trace("Unifying {} to {}", bindings, target);
        PixyPipe ans = unifyVars(bindings, target, clause, replacements);

        // Then apply the clauses. The order of application will searched using
        // brute-force till all the items are able to produce pipes
        int[] numClausesByCuts = splitClausesByCuts(clause.getBody());
        int totPermutation = factorial(numClausesByCuts);

        boolean success = false;
        Map<String, PixyDatum> rCopy = new HashMap<String, PixyDatum>(replacements);
        List<PixyPipe> steps = new ArrayList<PixyPipe>();
        for (int permutation = 0; permutation < totPermutation; permutation++) {
            success = true;

            varGen.mark();
            if (permutation > 0) {
                // Reset state
                steps.clear();
                replacements.clear();
                replacements.putAll(rCopy);
            }

            for (PixyDatum bodyItem : permute(permutation, clause.getBody(), numClausesByCuts)) {
                log.trace("Permutation #{} for clause: {}", permutation, clause);

                bodyItem = bodyItem.replaceVars(replacements);

                PixyPipe step;
                switch (bodyItem.getType()) {
                    case SPECIAL_ATOM:
                        if (bodyItem.isFail()) {
                            step = new FailPipe();
                        } else if (bodyItem.isTrue()) {
                            step = new NoopPipe();
                        } else if (bodyItem.isCut()) {
                            // The pixy cut named step is used by the merge to determine if the next clause should be
                            // evaluated
                            String varName = varGen.newVariable(PixySplitMergeStep.PIXY_CUT_PREFIX);

                            // A cut only lets one match through
                            step = new CutPipe(varName);
                        } else {
                            throw new PixyException(
                                    PixyErrorCodes.INTERNAL_ERROR, "Unhandled special atom " + bodyItem);
                        }
                        break;

                    case RELATION:
                        step = convertRuleToPixyPipe(
                                bodyItem.getRelationName(), bodyItem.getRelationTuple(), varGen, replacements);
                        break;

                    case NUMBER:
                    case STRING:
                    case VARIABLE:
                    case LIST:
                    default:
                        throw new PixyException(PixyErrorCodes.INTERNAL_ERROR, "Unexpected body term: " + bodyItem);
                }

                if (step == null) {
                    // Failure!
                    varGen.reset();
                    success = false;
                    break;
                }

                steps.add(step);
            }

            if (success) {
                break;
            }
        }

        if (success) {
            // Make sure that no vars are dangling, i.e., unused inside the clause
            // This doesn't apply to not()
            if (!clause.getHead().getRelationName().startsWith("__not")) {
                for (PixyDatum binding : bindings) {
                    String varName = binding.getVarName();
                    if (binding.getType() == PixyDatumType.VARIABLE) {
                        while ((binding = replacements.get(binding.getVarName())) != null) {
                            if (binding.isGround()) {
                                break;
                            }
                        }

                        if (binding == null) {
                            throw new PixyException(
                                    PixyErrorCodes.SEQUENCING_ERROR,
                                    "Variable " + varName + " can not be computed in the clause: " + clause
                                            + ". Replacements " + replacements);
                        }
                    }
                }
            }

            for (PixyPipe step : steps) {
                ans = connectPipe(ans, step);
            }

            if (ans == null) {
                return new NoopPipe();
            } else {
                return ans;
            }
        } else {
            return null;
        }
    }

    private List<PixyDatum> permute(int permutation, List<PixyDatum> list, int[] numClausesByCuts) {
        List<PixyDatum> ans = new ArrayList<PixyDatum>();
        int startIndex = 0;
        int endIndex = -1;

        for (int i = 0; i < numClausesByCuts.length; i++) {
            if (i > 0) {
                ans.add(new PixyDatum(PixyDatumType.SPECIAL_ATOM, PixyDatum.CUT));
            }

            int n = numClausesByCuts[i];
            int fact = factorial(n);
            endIndex = startIndex + n;

            List<PixyDatum> subList = permute((permutation % fact), list.subList(startIndex, endIndex));
            ans.addAll(subList);

            // Next sublist across a !
            permutation = permutation / fact;
            startIndex = endIndex + 1;
        }

        assert endIndex == list.size();

        return ans;
    }

    private List<PixyDatum> permute(int permutation, List<PixyDatum> list) {
        // The order is as follows: 0 => in order, N! - 1 => reverse order
        // First take reminder by N and divide by N
        list = new ArrayList<PixyDatum>(list);
        int n = list.size();
        List<PixyDatum> ans = new ArrayList<PixyDatum>();
        for (int i = 0; i < n; i++) {
            int opts = n - i;

            ans.add(list.remove(permutation % opts));
            permutation = permutation / opts;
        }

        assert list.size() == 0;

        return ans;
    }

    private int[] splitClausesByCuts(List<PixyDatum> body) {
        int[] ans = new int[0];
        int counter = 0;
        for (PixyDatum bodyItem : body) {
            if (bodyItem.isCut()) {
                ans = Arrays.copyOf(ans, ans.length + 1);
                ans[ans.length - 1] = counter;
                counter = 0;
            } else {
                counter++;
            }
        }

        ans = Arrays.copyOf(ans, ans.length + 1);
        ans[ans.length - 1] = counter;

        return ans;
    }

    private int factorial(int[] numClausesByCuts) {
        int ans = 1;
        for (int n : numClausesByCuts) {
            ans *= factorial(n);
        }
        return ans;
    }

    private int factorial(int n) {
        int ans = 1;
        for (int i = 1; i <= n; i++) {
            ans *= i;
        }
        return ans;
    }

    private static PixyPipe connectPipe(PixyPipe fromPipe, PixyPipe step) {
        if (fromPipe != null) {
            return new ConnectPipe(fromPipe, step);
        } else if (step != null) {
            return step;
        } else {
            return null;
        }
    }

    private PixyPipe unifyVars(
            List<PixyDatum> bindings, List<PixyDatum> targets, PixyClause clause, Map<String, PixyDatum> replacements) {
        PixyPipe ans = null;
        int arity = bindings.size();

        for (int i = 0; i < arity; i++) {
            PixyDatum binding = bindings.get(i);
            PixyDatum target = targets.get(i);

            ans = unifyVar(replacements, ans, binding, target);
        }

        // Replace stuff
        clause.replaceVars(replacements);

        return ans;
    }

    public static PixyPipe unifyVar(
            Map<String, PixyDatum> replacements, PixyPipe inputPipe, PixyDatum binding, PixyDatum target) {
        if (!target.isGround()) {
            assert (target.getType() == PixyDatumType.VARIABLE);
            replacements.put(target.getVarName(), binding);
            return inputPipe;
        } else if (!binding.isGround()) {
            assert (binding.getType() == PixyDatumType.VARIABLE);
            replacements.put(
                    binding.getVarName(), new PixyDatum(PixyDatumType.SPECIAL_ATOM, "$" + binding.getVarName()));
            return connectPipe(inputPipe, new EvalPipe(binding.getVarName(), Arrays.asList(new PixyDatum[] {target})));
        } else {
            // Both are ground
            return connectPipe(inputPipe, equalsPipe(binding, target));
        }
    }

    protected static PixyPipe equalsPipe(PixyDatum lhs, PixyDatum rhs) {
        // At this point both lhs and rhs are 'ground'
        if (lhs.isFixed() && rhs.isFixed()) {
            if (lhs.equals(rhs)) {
                // All OK
                return null;
            } else {
                return new FailPipe();
            }
        } else if (!lhs.isFixed()) {
            return new FilterPipe(lhs.getAtomVarName(), rhs);
        } else {
            assert !rhs.isFixed();
            return new FilterPipe(rhs.getAtomVarName(), lhs);
        }
    }
}
