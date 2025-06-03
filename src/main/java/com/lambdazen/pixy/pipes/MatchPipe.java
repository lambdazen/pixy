package com.lambdazen.pixy.pipes;

import com.lambdazen.pixy.PixyDatum;
import com.lambdazen.pixy.PixyDatumType;
import com.lambdazen.pixy.PixyPipe;

public class MatchPipe extends FilterPipe implements PixyPipe, NamedInputPipe, InternalLookupPipe {
    public MatchPipe(String varName) {
        this(null, varName);
    }

    public MatchPipe(String varName1, String varName2) {
        super(varName1, new PixyDatum(PixyDatumType.SPECIAL_ATOM, "$" + varName2));
    }
}
