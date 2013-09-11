package com.lambdazen.pixy.gremlin;

import com.lambdazen.pixy.PixyDatum;
import com.lambdazen.pixy.PixyErrorCodes;
import com.lambdazen.pixy.PixyException;
import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.transform.TransformPipe;
import com.tinkerpop.pipes.util.AsPipe;
import com.tinkerpop.pipes.util.PipeHelper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class PixyEvalPipe<S, T> extends AbstractPipe<S, T> implements TransformPipe<S, T>, PixyParentQueryPipe {
    private static Map<String, EvalOperation> EVAL_OPS;

    private List<PixyDatum> operations;
    private final Map<String, AsPipe> asPipeMap;
    private List<AsPipe> allAsPipes;
    private PixyParentPipe parentPipe;
    private boolean initialized = false;
    
    static {
    	EVAL_OPS = new HashMap<String, EvalOperation>();
    	
    	loadEvalOps();
    }

    public PixyEvalPipe(List<PixyDatum> operations, List<AsPipe> allPreviousAsPipes) {
    	this.operations = operations;
    	this.asPipeMap = new HashMap<String, AsPipe>();
    	this.allAsPipes = new ArrayList<AsPipe>(allPreviousAsPipes);
    }
    
    private void loadPipesIfNecessary() {
    	if (!initialized) {
	    	initialized = true;
	    	
	    	if (parentPipe != null) {
	    		allAsPipes.addAll(parentPipe.getAsPipes());
	    	}
	    	
	    	// Scope the steps so that the last as() is picked up
	    	Collections.reverse(allAsPipes);
	
	    	for (PixyDatum op : operations) {
	    		if (op.isAtomAPipeVar()) {
	    			String namedStep = op.getAtomVarName();
	
	    			if (namedStep.length() > 0) {
	    				asPipeMap.put(namedStep, findNamedStep(namedStep));
	    			}
	    		}
	    	}
    	}
    }

    public void setParentPipe(PixyParentPipe parent) {
    	this.parentPipe = parent;
    }

    private AsPipe findNamedStep(String namedStep) {
    	for (AsPipe asPipe : allAsPipes) {
    		if (asPipe.getName().equals(namedStep)) {
    			return asPipe;
    		}
    	}
    	
    	throw new PixyException(PixyErrorCodes.MISSING_NAMED_STEP, "Could not find named step: " + namedStep + " in " + allAsPipes);
	}

	public T processNextStart() {
		// The pipe doesn't know its ancestors till the first event is received
		loadPipesIfNecessary();

		T ans = null;
		
		while (ans == null) {
			S input = this.starts.next();
	
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
	        				curEnd = input;
	        			} else {
							AsPipe asPipe = asPipeMap.get(namedStep);
		        			assert (asPipe != null); // The constructor should have made sure of that
		        			curEnd = asPipe.getCurrentEnd();
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
	        		throw new PixyException(PixyErrorCodes.INTERNAL_ERROR, "Unhandled operation: " + op + ". Run stack: " + runStack);
	        	}        	
	        }
	
	        if (runStack.size() == 1) {
	        	ans = (T)runStack.pop();
	        } else {
	            throw new PixyException(PixyErrorCodes.INTERNAL_ERROR, "Internal error. Operations " + operations + " left run stack with " + runStack);        	
	        }
		}
		
		return ans;
    }

    private Object convertToRuntimeObject(String namedStep, Object curEnd) {
		if (curEnd == null) { 
			throw new PixyException(PixyErrorCodes.COALESCE_FAILURE, "PixyEvalPipe encountered null in named step " + namedStep); 
		} else if (PixyDatum.isNumber(curEnd)) {
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
    	return PipeHelper.makePipeString(this, this.operations.toArray());
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
					return ((BigDecimal)operand1).add((BigDecimal)operand2);
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
			return (BigDecimal)pop;
		} else {
			throw new PixyException(PixyErrorCodes.TYPE_MISMATCH_IN_EVAL, "Encountered " + pop.getClass() + " while expecting a BigDecimal");
		}
	}

    private static Boolean asBoolean(Object pop) {
    	if (pop == null) {
    		throw new PixyException(PixyErrorCodes.INTERNAL_ERROR, "Encountered null");
    	} else if (pop instanceof Boolean) {
			return (Boolean)pop;
		} else {
			throw new PixyException(PixyErrorCodes.TYPE_MISMATCH_IN_EVAL, "Encountered " + pop.getClass() + " while expecting a BigDecimal");
		}
	}
}
