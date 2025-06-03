package com.lambdazen.pixy;

public class PixyException extends RuntimeException {
    private static final long serialVersionUID = -5310572247323732287L;
    PixyErrorCodes code;

    public PixyException(PixyErrorCodes code) {
        super(code.toString());

        this.code = code;
    }

    public PixyException(PixyErrorCodes code, String s) {
        super(code.toString() + ". " + s);

        this.code = code;
    }

    public PixyException(PixyErrorCodes code, String s, Throwable t) {
        super(code.toString() + ". " + s, t);

        this.code = code;
    }

    public PixyErrorCodes getErrorCode() {
        return code;
    }
}
