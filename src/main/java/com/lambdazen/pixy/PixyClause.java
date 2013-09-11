package com.lambdazen.pixy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.igormaznitsa.prologparser.terms.AbstractPrologTerm;
import com.igormaznitsa.prologparser.terms.PrologStructure;

/** This class captures a Horn clause in Prolog of the form head (:- (body)+)* */
public class PixyClause {
	private static Set<String> unsupportedOps = new HashSet<String>();
	private static Set<String> booleanOps = new HashSet<String>();
	private static Set<String> arithOps = new HashSet<String>();
	
	static {
		// TODO: Support -> (if then else)
		unsupportedOps.addAll(Arrays.asList(new String[] {
				"?-/1", ":-/1", ":-/2",                   // No goal questions
				"=../2", "->/2", "-->/2", "\\/1", "**/2", // No advanced ops
				"=:=/2", "=\\=/2", "\\==/2",              // Only == and \=
				"/\\/2", "\\//2", "<</2", ">>/2", "^/2",  // No binary ops
				 "is/2", "=/2"                    // Handled outside expressions 
		}));

		booleanOps.addAll(Arrays.asList(new String[] {
				";/2", ",/2", "==/2", "\\=/2",            
				"</2", "=</2", ">=/2", ">/2",             // Number comparisons 
				"@</2", "@=</2", "@>/2", "@>=/2",         // Alpha comparisons
				"\\+/1"
		}));

		arithOps.addAll(Arrays.asList(new String[] {
				"+/2", "-/2", "*/2", "//2", "-/1",          // Number ops
				"///2", "rem/2", "mod/2"                    // Integer ops
		}));
	}
	
	PixyDatum head;
	List<PixyDatum> body;

	public PixyClause(PixyDatum head, List<PixyDatum> body) {
		assert(head.getType() == PixyDatumType.RELATION);

		this.head = head;
		this.body = body;
	}
	
	public PixyClause(AbstractPrologTerm term) {
		this.body = new ArrayList<PixyDatum>();
		
		assert (term instanceof PrologStructure);

		PrologStructure structure = ((PrologStructure)term);

		if (structure.getText().equals(":-")) {
			this.head = new PixyDatum(structure.getElement(0));
			loadBody(structure.getElement(1));
		} else {
			this.head = new PixyDatum(structure);
		}
		
		for (PixyDatum bodyItem : body) {
			if ((bodyItem.getType() != PixyDatumType.RELATION) && (bodyItem.getType() != PixyDatumType.SPECIAL_ATOM)) {
				throw new PixyException(PixyErrorCodes.INVALID_TERM_IN_CLAUSE_BODY, "Encountered: " + bodyItem);
			}
		}
	}

	public void loadBody(AbstractPrologTerm term) {
		if (term instanceof PrologStructure) {
			PrologStructure structure = ((PrologStructure)term);

			if (structure.getText().equals(",")) {
				loadBody(structure.getElement(0));
				loadBody(structure.getElement(1));
			} else if (structure.getText().equals("=")){
				loadEqualsExpression(structure.getElement(0), structure.getElement(1));
			} else if (booleanOps.contains(structure.getText() + "/" + structure.getArity())) {
				loadBooleanExpression(structure);
			} else if (structure.getText().equals("is")) {
				loadIsExpression(structure.getElement(0), structure.getElement(1));
			} else if (arithOps.contains(structure.getText() + "/" + structure.getArity())) {
				throw new PixyException(PixyErrorCodes.OPERATION_NOT_SUPPORTED_IN_CONTEXT, "The operator " + structure.getText() + " is not supported as a clause. Try <Var> is <Expression>");
			} else if (unsupportedOps.contains(structure.getText() + "/" + structure.getArity())) {
				throw new PixyException(PixyErrorCodes.OPERATION_NOT_SUPPORTED, "Encountered " + structure.getText() + " with arity " + structure.getArity());
			} else {
				body.add(new PixyDatum(structure));
			}
		} else {
			body.add(new PixyDatum(term));
		}
	}

	private void loadBooleanExpression(PrologStructure structure) {
		// The term must be converted to a list
		List<PixyDatum> list = new ArrayList<PixyDatum>();
		convertExpressionToList(structure , list);
		PixyDatum param = new PixyDatum(PixyDatumType.LIST, list);
		
		body.add(new PixyDatum(PixyDatumType.RELATION, "(bool)", Arrays.asList(new PixyDatum[] {param})));
	}

	private void loadIsExpression(AbstractPrologTerm lhsTerm, AbstractPrologTerm rhsTerm) {
		PixyDatum lhs = new PixyDatum(lhsTerm);
		
		if ((lhs.getType() == PixyDatumType.RELATION) 
				|| (lhs.getType() == PixyDatumType.LIST)){
			throw new PixyException(PixyErrorCodes.CAN_NOT_UNIFY_RELATIONS_OR_LISTS, "Encountered " + lhsTerm + " is " + rhsTerm);
		}
		
		// The RHS term must be converted to a list
		List<PixyDatum> list = new ArrayList<PixyDatum>();
		convertExpressionToList(rhsTerm , list);
		PixyDatum rhs = new PixyDatum(PixyDatumType.LIST, list);
		
		PixyDatum[] params = new PixyDatum[] {lhs, rhs};
		body.add(new PixyDatum(PixyDatumType.RELATION, "(is)", Arrays.asList(params)));
	}

	private void convertExpressionToList(AbstractPrologTerm term , List<PixyDatum> list) {
		// Convert term to postfix and add it to the list
		switch (term.getType()) {
		case VAR:
		case ATOM:
			list.add(new PixyDatum(term));
			break;
			
		case STRUCT:
			PrologStructure struct = (PrologStructure)term;
			int arity = struct.getArity();
			String operation = struct.getText() + "/" + arity;

			if ((booleanOps.contains(operation)) || (arithOps.contains(operation))) {
				// This is postfix
				for (int i=0; i < arity; i++) {
					convertExpressionToList(struct.getElement(i), list);
				}

				list.add(new PixyDatum(PixyDatumType.SPECIAL_ATOM, operation));				
			} else if (unsupportedOps.contains(operation)) {
				throw new PixyException(PixyErrorCodes.OPERATION_NOT_SUPPORTED, "Encountered " + operation + " in " + struct);
			} else {
				throw new PixyException(PixyErrorCodes.INTERNAL_ERROR, "Unable to parse operation " + operation + " in " + struct);
			}

			break;

		default:
			throw new PixyException(PixyErrorCodes.INVALID_TERM_IN_EXPRESSION, "Encountered: " + term);
		}
	}

	private void loadEqualsExpression(AbstractPrologTerm lhsTerm, AbstractPrologTerm rhsTerm) {
		PixyDatum lhs = new PixyDatum(lhsTerm);
		PixyDatum rhs = new PixyDatum(rhsTerm);
		
		if ((lhs.getType() == PixyDatumType.RELATION) || (rhs.getType() == PixyDatumType.RELATION)) {
			throw new PixyException(PixyErrorCodes.CAN_NOT_UNIFY_RELATIONS_OR_LISTS, "Encountered " + lhsTerm + " = " + rhsTerm);
		}
		
		body.add(new PixyDatum(PixyDatumType.RELATION, "(=)", Arrays.asList(lhs, rhs)));
	}

	public String getRelationSignature() {
		return head.getRelationName() + "/" + head.getArity();
	}
	
	public PixyDatum getHead() {
		return head;
	}
	
	public List<PixyDatum> getBody() {
		return body;
	}
	
	public PixyClause renameToUniqueVars(VariableGenerator varGen) {
		Map<String, String> varMap = new HashMap<String, String>();
		PixyDatum newHead = head.renameToUniqueVars(varMap, varGen);
		
		List<PixyDatum> newBody = new ArrayList<PixyDatum>();
		for (PixyDatum bodyItem : body) {
			newBody.add(bodyItem.renameToUniqueVars(varMap, varGen));
		}
		
		PixyClause ans = new PixyClause(newHead, newBody);
		return ans;
	}

	public void replaceVars(Map<String, PixyDatum> replacements) {
		if (replacements.size() > 0) {
			head = head.replaceVars(replacements);
			
			List<PixyDatum> newBody = new ArrayList<PixyDatum>();
			for (PixyDatum bodyItem : body) {
				newBody.add(bodyItem.replaceVars(replacements));
			}

			body = newBody;
		}
	}
	
	public String toString() {
		return head + " :- " + body;
	}
}
