package com.lambdazen.pixy;

import static com.igormaznitsa.prologparser.ParserContext.FLAG_BLOCK_COMMENTS;
import static com.igormaznitsa.prologparser.ParserContext.FLAG_CURLY_BRACKETS;
import static com.igormaznitsa.prologparser.ParserContext.FLAG_ZERO_QUOTATION_CHARCODE;

import com.igormaznitsa.prologparser.DefaultParserContext;
import com.igormaznitsa.prologparser.GenericPrologParser;
import com.igormaznitsa.prologparser.terms.PrologTerm;
import com.igormaznitsa.prologparser.tokenizer.Op;
import com.lambdazen.pixy.pipemakers.*;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PixyTheory {
    public static final PipeMaker[] DEFAULT_PREDEFINED_RULES = new PipeMaker[] {
        new Out3(),
        new Out2(),
        new OutV2(),
        new OutE2(),
        new OutE3(),
        new In3(),
        new In2(),
        new InV2(),
        new InE2(),
        new InE3(),
        new Both2(),
        new Both3(),
        new BothV2(),
        new BothE2(),
        new BothE3(),
        new OutLoop2(),
        new OutLoop3(),
        new OutLoop4(),
        new InLoop2(),
        new InLoop3(),
        new InLoop4(),
        new BothLoop2(),
        new BothLoop3(),
        new BothLoop4(),
        new Property3(),
        new Property4(),
        new Label2(),
        new Is2(),
        new Unify2(),
        new Bool1()
    };

    Map<String, PipeMaker> predefinedRules;
    private Map<String, List<PixyClause>> rules;

    public PixyTheory(String theory) {
        this(theory, null);
    }

    protected PixyTheory(String theory, Map<String, List<PixyClause>> oldRules) {
        this.predefinedRules = new HashMap<String, PipeMaker>();
        addPredefinedRules(DEFAULT_PREDEFINED_RULES);

        if (oldRules == null) {
            this.rules = new HashMap<String, List<PixyClause>>();
        } else {
            this.rules = new HashMap<String, List<PixyClause>>(oldRules);
        }

        try (GenericPrologParser parser = new GenericPrologParser(
                new StringReader(theory),
                DefaultParserContext.of(
                        FLAG_BLOCK_COMMENTS | FLAG_ZERO_QUOTATION_CHARCODE | FLAG_CURLY_BRACKETS, Op.SWI))) {
            while (parser.hasNext()) {
                PrologTerm prologTerm = parser.next();
                PixyClause pc = new PixyClause(prologTerm);
                String signature = pc.getRelationSignature();

                if (!rules.containsKey(signature)) {
                    rules.put(signature, new ArrayList<>());
                }

                rules.get(signature).add(pc);
            }
        } catch (Exception parseEx) {
            throw new PixyException(PixyErrorCodes.PROLOG_PARSE_ERROR, "", parseEx);
        }
    }

    public void addPredefinedRules(PipeMaker... rules) {
        for (PipeMaker pm : rules) {
            predefinedRules.put(pm.getSignature(), pm);
        }
    }

    public List<PixyClause> getClauses(String relationSignature) {
        return rules.get(relationSignature);
    }

    public List<String> getRelationSignatures() {
        List<String> ans = new ArrayList<String>(rules.keySet());
        Collections.sort(ans);
        return ans;
    }

    public PixyPipe makePipe(String query, Object... params) {
        PixyGrinder pg = new PixyGrinder(this);

        return pg.convertQueryToPipe(query, params);
    }

    public PixyTheory extend(String newRules) {
        return new PixyTheory(newRules, rules);
    }

    public PixyTheory remove(String... rulesToRemove) {
        Map<String, List<PixyClause>> rulesCopy = new HashMap<String, List<PixyClause>>(rules);

        for (String rule : rulesToRemove) {
            if (rulesCopy.remove(rule) == null) {
                throw new PixyException(
                        PixyErrorCodes.CAN_NOT_REMOVE_MISSING_RULE,
                        "Encountered " + rule + ". All available rules: " + rulesCopy.keySet());
            }
        }

        return new PixyTheory("", rulesCopy);
    }

    public Map<String, PipeMaker> getPredefinedRules() {
        return predefinedRules;
    }
}
