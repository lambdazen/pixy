package com.lambdazen.pixy;

import com.igormaznitsa.prologparser.terms.PrologAtom;
import com.igormaznitsa.prologparser.terms.PrologFloat;
import com.igormaznitsa.prologparser.terms.PrologInt;
import com.igormaznitsa.prologparser.terms.PrologList;
import com.igormaznitsa.prologparser.terms.PrologStruct;
import com.igormaznitsa.prologparser.terms.PrologTerm;
import com.igormaznitsa.prologparser.terms.PrologVar;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** This class captures a data element in Pixy. It could be a "special atom", relation, string, number, variable or list. */
public class PixyDatum {
    public static final int BIGDECIMAL_SCALE = 10;

    public static final String FAIL = "fail";
    public static final String TRUE = "true";
    public static final String FALSE = "false";
    public static final String CUT = "!";

    private PixyDatumType type;
    private BigDecimal num;
    private String str;
    private String var;
    private String atom;
    private List<PixyDatum> list;
    private String relName;
    private List<PixyDatum> relTuple;

    public PixyDatum(PixyDatumType type, BigDecimal num) {
        assert type == PixyDatumType.NUMBER;
        this.type = type;
        this.num = num;
    }

    public PixyDatum(PixyDatumType type, String s) {
        this.type = type;
        if (type == PixyDatumType.VARIABLE) {
            this.var = s;
        } else if (type == PixyDatumType.SPECIAL_ATOM) {
            this.atom = s;
        } else if (type == PixyDatumType.STRING) {
            this.str = s;
        } else {
            assert true;
        }
    }

    public PixyDatum(PixyDatumType type, List<PixyDatum> list) {
        assert type == PixyDatumType.LIST;
        this.type = type;
        this.list = list;
    }

    public PixyDatum(PixyDatumType type, String relName, List<PixyDatum> relTuple) {
        assert type == PixyDatumType.RELATION;

        this.type = type;
        this.relName = relName;
        this.relTuple = relTuple;
    }

    public PixyDatum(PrologTerm term) {
        if ((term instanceof PrologInt) || (term instanceof PrologFloat)) {
            this.type = PixyDatumType.NUMBER;
            this.num = new BigDecimal(term.getText());
        } else if (term instanceof PrologAtom) {
            // Special atom handling
            if (term.getText().equals(FAIL)) {
                this.type = PixyDatumType.SPECIAL_ATOM;
                this.atom = FAIL;
            } else if (term.getText().equals(FALSE)) {
                this.type = PixyDatumType.SPECIAL_ATOM;
                this.atom = FALSE;
            } else if (term.getText().equals(TRUE)) {
                this.type = PixyDatumType.SPECIAL_ATOM;
                this.atom = TRUE;
            } else {
                this.type = PixyDatumType.STRING;
                this.str = term.getText();
            }
        } else if (term instanceof PrologVar) {
            this.type = PixyDatumType.VARIABLE;
            this.var = term.getText();
        } else if (term instanceof PrologList) {
            PrologList pList = (PrologList) term;
            this.type = PixyDatumType.LIST;
            this.list = new ArrayList<PixyDatum>();

            while (!pList.isEmpty()) {
                PrologTerm pHead = pList.getHead();

                if (pHead instanceof PrologVar) {
                    throw new PixyException(
                            PixyErrorCodes.VARIABLES_NOT_SUPPORTED_INSIDE_LISTS,
                            "Encountered variable " + pHead.getText() + " in " + term.toString());
                }

                list.add(new PixyDatum(pList.getHead()));

                pList = (PrologList) pList.getTail();
            }
        } else if ((term instanceof PrologStruct) && term.getText().equals("!")) {
            this.type = PixyDatumType.SPECIAL_ATOM;
            this.atom = CUT;
        } else if (term instanceof PrologStruct) {
            this.type = PixyDatumType.RELATION;
            this.relName = term.getText();
            this.relTuple = new ArrayList<PixyDatum>();

            PrologStruct structure = ((PrologStruct) term);

            // Check for not
            if (relName.equals("not")) {
                if (structure.getArity() != 1) {
                    throw new PixyException(
                            PixyErrorCodes.INVALID_NOT_PARAMETERS,
                            "Encountered: " + structure.toString() + " with arity " + structure.getArity());
                }

                // not(something(with, params))
                PrologTerm notTerm = structure.getTermAt(0);
                if (notTerm instanceof PrologStruct) {
                    // The relation names of the form not(something) will be handled internally
                    relName = "not(" + notTerm.getText() + ")";
                    structure = (PrologStruct) notTerm;
                } else {
                    throw new PixyException(
                            PixyErrorCodes.INVALID_NOT_PARAMETERS,
                            "Encountered: " + structure.toString() + " which is not a relation");
                }
            }

            // Go over the terms
            for (int idx = 0; idx < structure.getArity(); idx++) {
                PrologTerm subStruct = structure.getTermAt(idx);
                PixyDatum subExpr = new PixyDatum(subStruct);
                PixyDatumType subExprType = subExpr.getType();
                if ((subExprType != PixyDatumType.SPECIAL_ATOM)
                        && (subExprType != PixyDatumType.STRING)
                        && (subExprType != PixyDatumType.NUMBER)
                        && (subExprType != PixyDatumType.VARIABLE)
                        && (subExprType != PixyDatumType.LIST)) {
                    throw new PixyException(
                            PixyErrorCodes.ENCOUNTERED_COMPLEX_RELATION,
                            "Encountered: " + subStruct.toString() + " inside " + structure.toString());
                } else {
                    relTuple.add(subExpr);
                }
            }
        }
    }

    public PixyDatumType getType() {
        return type;
    }

    public BigDecimal getNumber() {
        return num;
    }

    public String getRelationName() {
        return relName;
    }

    public int getArity() {
        return relTuple.size();
    }

    public List<PixyDatum> getRelationTuple() {
        return relTuple;
    }

    public boolean isFail() {
        return (getType() == PixyDatumType.SPECIAL_ATOM) && (atom.equals(FAIL) || atom.equals(FALSE));
    }

    public boolean isTrue() {
        return (getType() == PixyDatumType.SPECIAL_ATOM) && atom.equals(TRUE);
    }

    public boolean isCut() {
        return (getType() == PixyDatumType.SPECIAL_ATOM) && atom.equals("!");
    }

    public boolean isGround() {
        return (type == PixyDatumType.NUMBER)
                || (type == PixyDatumType.SPECIAL_ATOM)
                || (type == PixyDatumType.STRING)
                || (type == PixyDatumType.LIST);
    }

    public boolean isFixed() {
        return (type == PixyDatumType.NUMBER)
                || ((type == PixyDatumType.SPECIAL_ATOM) && !isAtomAPipeVar())
                || (type == PixyDatumType.STRING)
                || (type == PixyDatumType.LIST);
    }

    public String getVarName() {
        return var;
    }

    public PixyDatum renameToUniqueVars(Map<String, String> varMap, VariableGenerator varGen) {
        switch (type) {
            case NUMBER:
            case SPECIAL_ATOM:
            case STRING:
                return this;

            case LIST:
                // Rename its items
                List<PixyDatum> newList = new ArrayList<PixyDatum>();
                for (PixyDatum listItem : list) {
                    newList.add(listItem.renameToUniqueVars(varMap, varGen));
                }
                return new PixyDatum(PixyDatumType.LIST, newList);

            case VARIABLE:
                // Return the mapped variable, if defined, or a new one
                String mappedVar = varMap.get(var);
                if (mappedVar == null) {
                    mappedVar = varGen.newVariable(var);

                    // The variable _ should always be renamed to unique
                    if (!var.equals("_")) {
                        varMap.put(var, mappedVar);
                    }
                }

                return new PixyDatum(PixyDatumType.VARIABLE, mappedVar);

            case RELATION:
                List<PixyDatum> tuple = new ArrayList<PixyDatum>();
                for (PixyDatum field : relTuple) {
                    tuple.add(field.renameToUniqueVars(varMap, varGen));
                }

                return new PixyDatum(PixyDatumType.RELATION, relName, tuple);

            default:
                throw new PixyException(PixyErrorCodes.INTERNAL_ERROR, "Unhandled type " + type);
        }
    }

    public PixyDatum replaceVars(Map<String, PixyDatum> replacements) {
        switch (type) {
            case NUMBER:
            case SPECIAL_ATOM:
            case STRING:
                return this;

            case LIST:
                // Replace its items
                List<PixyDatum> newList = new ArrayList<PixyDatum>();
                for (PixyDatum listItem : list) {
                    newList.add(listItem.replaceVars(replacements));
                }
                return new PixyDatum(PixyDatumType.LIST, newList);

            case VARIABLE:
                // Return the mapped variable, if defined, or a new one
                PixyDatum replacement = replacements.get(var);
                if (replacement == null) {
                    return this;
                } else {
                    // Using recursion to replace more than once
                    return replacement.replaceVars(replacements);
                }

            case RELATION:
                List<PixyDatum> tuple = new ArrayList<PixyDatum>();
                for (PixyDatum field : relTuple) {
                    tuple.add(field.replaceVars(replacements));
                }

                return new PixyDatum(PixyDatumType.RELATION, relName, tuple);

            default:
                throw new PixyException(PixyErrorCodes.INTERNAL_ERROR, "Unhandled type " + type);
        }
    }

    public String toString() {
        switch (type) {
            case NUMBER:
                return num.toString();

            case SPECIAL_ATOM:
                return atom;

            case STRING:
                return "'" + str + "'";

            case VARIABLE:
                return var;

            case RELATION:
                return relName + relTuple;

            case LIST:
                return list.toString();

            default:
                throw new PixyException(PixyErrorCodes.INTERNAL_ERROR, "Unhandled type " + type);
        }
    }

    public boolean isAtomAPipeVar() {
        return (type == PixyDatumType.SPECIAL_ATOM) && atom.startsWith("$");
    }

    public String getAtom() {
        return atom;
    }

    public String getAtomVarName() {
        assert isAtomAPipeVar();

        return atom.substring(1);
    }

    public String getString() {
        return str;
    }

    public List<PixyDatum> getList() {
        return list;
    }

    public static boolean isNumber(Object x) {
        return (x instanceof Integer)
                || (x instanceof Long)
                || (x instanceof Short)
                || (x instanceof Byte)
                || (x instanceof BigInteger)
                || (x instanceof Float)
                || (x instanceof Double);
    }

    public static BigDecimal convertToBigDecimal(Object x) {
        if (x instanceof Integer) {
            return new BigDecimal((Integer) x);
        } else if (x instanceof Long) {
            return new BigDecimal((Long) x);
        } else if (x instanceof BigInteger) {
            return new BigDecimal((BigInteger) x);
        } else if (x instanceof Float) {
            return new BigDecimal((Float) x);
        } else if (x instanceof Double) {
            return new BigDecimal((Double) x);
        } else if (x instanceof Byte) {
            return new BigDecimal((Byte) x);
        } else if (x instanceof Short) {
            return new BigDecimal((Short) x);
        } else {
            try {
                return new BigDecimal(x.toString());
            } catch (NumberFormatException e) {
                throw new PixyException(PixyErrorCodes.INVALID_NUMBER, "Encountered: " + x);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PixyDatum) {
            PixyDatum pd = (PixyDatum) o;

            return type.equals(pd.getType()) && toString().equals(pd.toString());
        } else {
            return false;
        }
    }
}
