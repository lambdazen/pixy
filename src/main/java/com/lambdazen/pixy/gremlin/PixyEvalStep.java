package com.lambdazen.pixy.gremlin;

import com.lambdazen.pixy.PixyDatum;
import com.lambdazen.pixy.PixyErrorCodes;
import com.lambdazen.pixy.PixyException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser.Admin;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class PixyEvalStep extends PixyCoalesceStep {
    private static Map<String, EvalOperation> EVAL_OPS;

    private List<PixyDatum> operations;
    private boolean initialized = false;

    static {
        EVAL_OPS = new HashMap<String, EvalOperation>();

        loadEvalOps();
    }

    public PixyEvalStep(final Traversal.Admin traversal, List<PixyDatum> operations) {
        super(traversal, new String[0]);
        this.operations = operations;
    }

    @Override
    protected Object map(Admin traverser) {
        Object ans = null;

        // Execute the operations
        Stack<Object> runStack = new Stack<Object>();

        for (PixyDatum op : operations) {
            switch (op.getType()) {
                case NUMBER:
                    runStack.push(op.getNumber());
                    break;

                case STRING:
                    runStack.push(op.getString());
                    break;

                case SPECIAL_ATOM:
                    if (op.isAtomAPipeVar()) {
                        String namedStep = op.getAtomVarName();

                        Object curEnd;
                        if (namedStep.equals("")) {
                            curEnd = traverser.get();
                        } else {
                            curEnd = this.getNullableScopeValue(null, namedStep, traverser);
                        }

                        Object item = convertToRuntimeObject(namedStep, curEnd);
                        runStack.push(item);
                    } else if (op.isTrue()) {
                        runStack.push(Boolean.TRUE);
                    } else if (op.isFail()) {
                        runStack.push(Boolean.FALSE);
                    } else {
                        runStack.push(calculate(op.getAtom(), runStack));
                    }
                    break;

                case RELATION:
                case LIST:
                case VARIABLE:
                default:
                    throw new PixyException(
                            PixyErrorCodes.INTERNAL_ERROR, "Unhandled operation: " + op + ". Run stack: " + runStack);
            }
        }

        if (runStack.size() == 1) {
            ans = runStack.pop();
        } else {
            throw new PixyException(
                    PixyErrorCodes.INTERNAL_ERROR,
                    "Internal error. Operations " + operations + " left run stack with " + runStack);
        }

        return ans;
    }

    private Object convertToRuntimeObject(String namedStep, Object curEnd) {
        if (curEnd == null) {
            throw new PixyException(
                    PixyErrorCodes.COALESCE_FAILURE, "PixyEvalPipe encountered null in named step " + namedStep);
        }

        if (curEnd instanceof Property) {
            curEnd = ((Property) curEnd).value();
        }

        if (PixyDatum.isNumber(curEnd)) {
            return PixyDatum.convertToBigDecimal(curEnd);
        } else {
            return curEnd;
        }
    }

    private Object calculate(String op, Stack<Object> runStack) {
        EvalOperation evalOp = EVAL_OPS.get(op);

        if (evalOp == null) {
            throw new PixyException(PixyErrorCodes.INTERNAL_ERROR, "Unsupported operation: " + op);
        } else {
            return evalOp.eval(runStack);
        }
    }

    public void reset() {
        super.reset();
    }

    public String toString() {
        return StringFactory.stepString(this, this.operations);
    }

    private interface EvalOperation {
        public Object eval(Stack<Object> runStack);
    }

    private static void loadEvalOps() {
        EVAL_OPS.clear();

        EVAL_OPS.put("+/2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                Object operand2 = runStack.pop();
                Object operand1 = runStack.pop();

                if ((operand1 instanceof BigDecimal) && (operand1 instanceof BigDecimal)) {
                    return ((BigDecimal) operand1).add((BigDecimal) operand2);
                } else {
                    return operand1.toString() + operand2.toString();
                }
            }
        });

        EVAL_OPS.put("-/2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                BigDecimal operand2 = asBigDecimal(runStack.pop());
                BigDecimal operand1 = asBigDecimal(runStack.pop());

                return operand1.subtract(operand2);
            }
        });

        EVAL_OPS.put("*/2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                BigDecimal operand2 = asBigDecimal(runStack.pop());
                BigDecimal operand1 = asBigDecimal(runStack.pop());

                return operand1.multiply(operand2);
            }
        });

        EVAL_OPS.put("//2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                BigDecimal operand2 = asBigDecimal(runStack.pop());
                BigDecimal operand1 = asBigDecimal(runStack.pop());

                return operand1.setScale(PixyDatum.BIGDECIMAL_SCALE).divide(operand2, RoundingMode.FLOOR);
            }
        });

        EVAL_OPS.put("///2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                BigDecimal operand2 = asBigDecimal(runStack.pop());
                BigDecimal operand1 = asBigDecimal(runStack.pop());

                return operand1.divide(operand2).setScale(0, RoundingMode.FLOOR);
            }
        });

        EVAL_OPS.put("rem/2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                BigInteger operand2 = asBigDecimal(runStack.pop()).toBigInteger();
                BigInteger operand1 = asBigDecimal(runStack.pop()).toBigInteger();

                return new BigDecimal(operand1.remainder(operand2));
            }
        });

        EVAL_OPS.put("mod/2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                BigInteger operand2 = asBigDecimal(runStack.pop()).toBigInteger();
                BigInteger operand1 = asBigDecimal(runStack.pop()).toBigInteger();

                return new BigDecimal(operand1.mod(operand2));
            }
        });

        EVAL_OPS.put("-/1", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                BigDecimal operand1 = asBigDecimal(runStack.pop());

                return operand1.negate();
            }
        });

        // BOOLEAN OPS

        EVAL_OPS.put(";/2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                Boolean operand2 = asBoolean(runStack.pop());
                Boolean operand1 = asBoolean(runStack.pop());

                return new Boolean(operand1.booleanValue() || operand2.booleanValue());
            }
        });

        EVAL_OPS.put(",/2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                Boolean operand2 = asBoolean(runStack.pop());
                Boolean operand1 = asBoolean(runStack.pop());

                return new Boolean(operand1.booleanValue() && operand2.booleanValue());
            }
        });

        EVAL_OPS.put("\\+/1", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                Boolean operand1 = asBoolean(runStack.pop());

                return new Boolean(!(operand1.booleanValue()));
            }
        });

        // COMPARISON OPS

        EVAL_OPS.put("==/2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                Object operand2 = runStack.pop();
                Object operand1 = runStack.pop();

                return new Boolean(operand1.equals(operand2));
            }
        });

        EVAL_OPS.put("\\=/2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                Object operand2 = runStack.pop();
                Object operand1 = runStack.pop();

                return new Boolean(!operand1.equals(operand2));
            }
        });

        EVAL_OPS.put("</2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                BigDecimal operand2 = asBigDecimal(runStack.pop());
                BigDecimal operand1 = asBigDecimal(runStack.pop());

                return new Boolean(operand1.compareTo(operand2) < 0);
            }
        });

        EVAL_OPS.put(">/2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                BigDecimal operand2 = asBigDecimal(runStack.pop());
                BigDecimal operand1 = asBigDecimal(runStack.pop());

                return new Boolean(operand1.compareTo(operand2) > 0);
            }
        });

        EVAL_OPS.put("=</2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                BigDecimal operand2 = asBigDecimal(runStack.pop());
                BigDecimal operand1 = asBigDecimal(runStack.pop());

                return new Boolean(operand1.compareTo(operand2) <= 0);
            }
        });

        EVAL_OPS.put(">=/2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                BigDecimal operand2 = asBigDecimal(runStack.pop());
                BigDecimal operand1 = asBigDecimal(runStack.pop());

                return new Boolean(operand1.compareTo(operand2) >= 0);
            }
        });

        EVAL_OPS.put("@</2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                String operand2 = runStack.pop().toString();
                String operand1 = runStack.pop().toString();

                return new Boolean(operand1.compareTo(operand2) < 0);
            }
        });

        EVAL_OPS.put("@>/2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                String operand2 = runStack.pop().toString();
                String operand1 = runStack.pop().toString();

                return new Boolean(operand1.compareTo(operand2) > 0);
            }
        });

        EVAL_OPS.put("@=</2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                String operand2 = runStack.pop().toString();
                String operand1 = runStack.pop().toString();

                return new Boolean(operand1.compareTo(operand2) <= 0);
            }
        });

        EVAL_OPS.put("@>=/2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                String operand2 = runStack.pop().toString();
                String operand1 = runStack.pop().toString();

                return new Boolean(operand1.compareTo(operand2) >= 0);
            }
        });

        // SPECIAL OP FOR FILTER

        EVAL_OPS.put("ifeq/2", new EvalOperation() {
            @Override
            public Object eval(Stack<Object> runStack) {
                Object operand2 = runStack.pop();
                Object operand1 = runStack.pop();

                if (operand1.equals(operand2)) {
                    return operand1;
                } else {
                    return null;
                }
            }
        });
    }

    private static BigDecimal asBigDecimal(Object pop) {
        if (pop == null) {
            throw new PixyException(PixyErrorCodes.INTERNAL_ERROR, "Encountered null");
        } else if (pop instanceof BigDecimal) {
            return (BigDecimal) pop;
        } else {
            throw new PixyException(
                    PixyErrorCodes.TYPE_MISMATCH_IN_EVAL,
                    "Encountered " + pop.getClass() + " while expecting a BigDecimal");
        }
    }

    private static Boolean asBoolean(Object pop) {
        if (pop == null) {
            throw new PixyException(PixyErrorCodes.INTERNAL_ERROR, "Encountered null");
        } else if (pop instanceof Boolean) {
            return (Boolean) pop;
        } else {
            throw new PixyException(
                    PixyErrorCodes.TYPE_MISMATCH_IN_EVAL,
                    "Encountered " + pop.getClass() + " while expecting a BigDecimal");
        }
    }
}
