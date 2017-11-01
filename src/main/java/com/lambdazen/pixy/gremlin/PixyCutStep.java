package com.lambdazen.pixy.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class PixyCutStep extends FilterStep {
	boolean wasCut;

	public PixyCutStep(final Traversal.Admin traversal) {
		super(traversal);
		this.wasCut = false;
	}

	@Override
    public void reset() {
        super.reset();

		this.wasCut = false;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this);
    }
	
	public boolean wasCut() {
		return wasCut;
	}

	@Override
	protected boolean filter(Traverser.Admin traverser) {
		if (wasCut) {
			throw FastNoSuchElementException.instance();
		} else {
			wasCut = true;
			return true;
		}
	}
}
