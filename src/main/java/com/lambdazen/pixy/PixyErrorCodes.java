package com.lambdazen.pixy;

public enum PixyErrorCodes {
    INTERNAL_ERROR {
        public String toString() {
            return "INTERNAL_ERROR: An internal error occurred";
        }
    },

    VARIABLES_NOT_SUPPORTED_INSIDE_LISTS {
        public String toString() {
            return "VARIABLES_NOT_SUPPORTED_INSIDE_LISTS: Pixy doesn't support variables inside lists";
        }
    },

    INVALID_NOT_PARAMETERS {
        public String toString() {
            return "NOT_ARITY_IS_NOT_ONE: Expecting exactly one parameter inside not(). The parameter must be a relation. ";
        }
    },

    ENCOUNTERED_COMPLEX_RELATION {
        public String toString() {
            return "ENCOUNTERED_COMPLEX_RELATION: Pixy does not support relations inside other relations, with the exception of not(relation()). All parameters to a relation must be simple strings, numbers, variables, lists or special atoms (fail, true, false). ";
        }
    },

    INVALID_NUMBER {
        public String toString() {
            return "INVALID_NUMBER: The given string could not be converted to a number";
        }
    },

    OPERATION_NOT_SUPPORTED {
        public String toString() {
            return "OPERATION_NOT_SUPPORTED: The given operation is not supported";
        }
    },

    OPERATION_NOT_SUPPORTED_IN_CONTEXT {
        public String toString() {
            return "OPERATION_NOT_SUPPORTED_IN_CONTEXT: The given operation is not supported in the current context";
        }
    },

    CAN_NOT_UNIFY_RELATIONS_OR_LISTS {
        public String toString() {
            return "CAN_NOT_UNIFY_RELATIONS_OR_LISTS: Pixy does not support unification of relations or lists. Please rewrite your '=' or 'is' rule";
        }
    },

    INVALID_TERM_IN_EXPRESSION {
        public String toString() {
            return "INVALID_TERM_IN_EXPRESSION: The given term is invalid inside an expression. Pixy only supports numbers, strings, variables and operations in its expressions";
        }
    },

    INVALID_TERM_IN_CLAUSE_BODY {
        public String toString() {
            return "INVALID_TERM_IN_CLAUSE_BODY: A clause body should only contain relations, boolean expressions or special atoms (like fail, true, false)";
        }
    },

    INVALID_QUERY_FORM {
        public String toString() {
            return "INVALID_QUERY_FORM: A Pixy query should be of the form <predicate>(<param 1>, ..., <param N). ";
        }
    },

    INVALID_QUERY_PAREMETER {
        public String toString() {
            return "INVALID_QUERY_PAREMETER: The parameters to a Pixy query should be one of ?, _, &, $, &<named step>, $<named step>";
        }
    },

    QUERY_PLACEHOLDER_MISMATCH {
        public String toString() {
            return "QUERY_PLACEHOLDER_MISMATCH: A call to makePipe(<query>, values...) did not supply the same number of values as the number of place-holders (represented as ?) in the query";
        }
    },

    CAN_NOT_REMOVE_MISSING_RULE {
        public String toString() {
            return "CAN_NOT_REMOVE_MISSING_RULE: A given rule can not be removed because it is missing in the current list of available rules";
        }
    },

    PROLOG_PARSE_ERROR {
        public String toString() {
            return "PROLOG_PARSE_ERROR: The Java PROLOG parser threw the given exception";
        }
    },

    RECURSION_NOT_SUPPORTED {
        public String toString() {
            return "RECURSION_NOT_SUPPORTED: Pixy does not support recursion (yet). Please remove the given cyclic dependency among rules";
        }
    },

    REFERENCE_TO_MISSING_RULE {
        public String toString() {
            return "REFERENCE_TO_MISSING_RULE: The given rule was referenced but not defined in the theory";
        }
    },

    SEQUENCING_ERROR {
        public String toString() {
            return "SEQUENCING_ERROR: Pixy tries to sequence the terms in the body of a clause, say Head :- rel1(Params1), rel2(Params2), ..., such that all the relations fully evaluate the variables passed to them. This sequencing process failed. Typically this error occurs because of typos in variable names or insufficient bound parameters passed to the rule";
        }
    },

    EXPECTING_A_PIPE {
        public String toString() {
            return "EXPECTING_A_PIPE: A pre-defined rule (such as in, out) is expecting a parameter to be a pipe";
        }
    },

    EXPECTING_A_STRING {
        public String toString() {
            return "EXPECTING_A_STRING: Expecting a parameter to be a string";
        }
    },

    EXPECTING_A_STRING_OR_LIST_OF_STRINGS {
        public String toString() {
            return "EXPECTING_A_STRING_OR_LIST_OF_STRINGS: Expecting a parameter (such as label) to be a string or a list-of-strings";
        }
    },

    EXPECTING_A_STRING_OR_NUMBER {
        public String toString() {
            return "EXPECTING_A_STRING_OR_NUMBER: Expecting a parameter to be a string or a number";
        }
    },

    EXPECTING_A_STRING_NUMBER_OR_BOOLEAN {
        public String toString() {
            return "EXPECTING_A_STRING_NUMBER_OR_BOOLEAN: Expecting a parameter to be a string, a number, or a boolean value";
        }
    },

    EXPECTING_A_NUMBER_OR_LIST_OF_TWO_NUMBERS {
        public String toString() {
            return "EXPECTING_A_NUMBER_OR_LIST_OF_TWO_NUMBERS: Expecting a parameter to be a number, or a list of two numbers";
        }
    },

    COALESCE_FAILURE {
        public String toString() {
            return "COALESCE_FAILURE: A coalesce or eval step failed because the referenced named step(s) don't have a value. Typically, this is caused by null values assigned to Pixy variables";
        }
    },

    MISSING_NAMED_STEP {
        public String toString() {
            return "MISSING_NAMED_STEP: The named step provided to a custom Gremlin pipe implemented in Pixy is not valid. This typically indicates a bug, or an invalid expression";
        }
    },

    TYPE_MISMATCH_IN_EVAL {
        public String toString() {
            return "TYPE_MISMATCH_IN_EVAL: An eval operation could not be performed because the types of the operands don't match the types supported by the operation. This is commonly an error in an expression";
        }
    },
}
