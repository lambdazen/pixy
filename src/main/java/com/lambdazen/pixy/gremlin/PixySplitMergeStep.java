package com.lambdazen.pixy.gremlin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class PixySplitMergeStep<S, E> extends AbstractStep<S, E> implements TraversalParent {
    public static final String PIXY_CUT_PREFIX = "__pixy_cut_";
    
	private final List<Traversal.Admin<S, E>> pipelines;
    private final List<List<PixyCutStep>> cutPipeList;
    private int numPipelines;
    private int current;
    private Traverser.Admin<S> nextStart;

    public PixySplitMergeStep(final Traversal.Admin<S, E> traversal, List<Traversal.Admin<S, E>> pipelines) {
    	super(traversal);

    	this.pipelines = pipelines;
        this.numPipelines = pipelines.size();
        this.cutPipeList = new ArrayList<List<PixyCutStep>>(numPipelines);
        this.current = 0;
        this.nextStart = null;

        for (Traversal.Admin<S, E> pipeline : pipelines) {
        	List<PixyCutStep> cutPipes = null;

        	for (Step step : pipeline.getSteps()) {
        		if (step instanceof PixyCutStep) {
        			PixyCutStep cutPipe = (PixyCutStep)step; 
	        		// This is a cut -- need to track it
	    			if (cutPipes == null) {
	    				cutPipes = new ArrayList<PixyCutStep>();
	    			}

	    			cutPipes.add(cutPipe);
        		}
        	}
        	
        	this.cutPipeList.add(cutPipes);
        }
    }

    public Traverser.Admin<E> processNextStart() throws NoSuchElementException {
    	if (nextStart == null) {
	    	// This is the first call to processNextStart()
    		
    		// Get the first start
	    	this.nextStart = this.starts.next();
	    	
	    	// Feed the first pipe
	        this.pipelines.get(0).addStart(nextStart.split());
    	}

    	// Go over the pipes in order and extract values
        while (true) {
        	Traversal.Admin<S, E> pipe = this.pipelines.get(this.current);

        	List<PixyCutStep> cutPipes = this.cutPipeList.get(this.current); 

        	if (pipe.hasNext()) {
            	// The current pipe has another element
        		Traverser.Admin<E> ans = pipe.nextTraverser();
        		return ans;
            } else {
                // Did it trigger a cut?
                boolean wasCut = wasCut(cutPipes);

                // The current pipe is done -- reset it
            	pipe.reset();

            	// Is this a cut
            	if (wasCut) {
            		// Done with the input -- move to the next one
            		this.current = 0;
            	} else {
	            	// Not a cut. Move to the next pipe
	                this.current = (this.current + 1) % this.numPipelines;
            	}

                if (this.current == 0) {
                	// Get the next start for pipe #0
                	// The start is a list of one element
                	this.nextStart = null;

                	// This may throw NoSuchElementException to indicate the end of processing
                	this.nextStart = this.starts.next();
                }

                assert (nextStart != null);

                // Feed the nextStart to the current pipe
                this.pipelines.get(this.current).addStart(nextStart.split());
            }
        }
    }

    private boolean wasCut(List<PixyCutStep> cutPipes) {
    	if (cutPipes == null) {
    		return false;
    	}

    	for (PixyCutStep cutPipe : cutPipes) {
			if (cutPipe.wasCut()) {
				return true;
			}
		}

		return false;
	}

    public String toString() {
        return StringFactory.stepString(this, this.pipelines);
    }
    
    @Override
    public void reset() {
        super.reset();

        this.current = 0;
        this.nextStart = null;

        for (Traversal.Admin<S, E> pipeline: pipelines) {
            pipeline.reset();
        }
    }

    public List getGlobalChildren() {
        return pipelines;
    }

    public List getLocalChildren() {
    	return Collections.EMPTY_LIST;
    }
}