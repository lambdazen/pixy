package com.lambdazen.pixy.gremlin;

import com.lambdazen.pixy.PixyErrorCodes;
import com.lambdazen.pixy.PixyException;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.transform.TransformPipe;
import com.tinkerpop.pipes.util.AsPipe;
import com.tinkerpop.pipes.util.PipeHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CoalescePipe<S, T> extends AbstractPipe<S, T> implements TransformPipe<S, T>, PixyParentQueryPipe {
    private final List<AsPipe> asPipes = new ArrayList<AsPipe>();
    private final Collection<String> stepNames;
	private List<AsPipe> allAsPipes;
    private PixyParentPipe parentPipe;
    private boolean initialized = false;

    public CoalescePipe(String stepName, List<AsPipe> allPreviousAsPipes) {
    	this(Arrays.asList(new String[] {stepName}), allPreviousAsPipes);
    }
    
    public CoalescePipe(Collection<String> stepNames, final List<AsPipe> allPreviousAsPipes) {
    	assert(stepNames != null);
        this.stepNames = stepNames;

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

	    	Set<String> stepNamesSet = new HashSet<String>(stepNames);
	    	for (AsPipe asPipe : allAsPipes) {
	    		for (String stepName : stepNames) {
	    			if (stepNamesSet.remove(asPipe.getName())) {
		                this.asPipes.add(asPipe);
		            }
		        }
	        }
	    	
	    	if (stepNamesSet.size() > 0) {
	    		throw new PixyException(PixyErrorCodes.MISSING_NAMED_STEP, "Some named steps referenced in a coalesce operation are missing. Missing steps: " + stepNamesSet + ". Looking for: " + stepNames);
	    	}
    	}
    }
    
    @Override
    public void setParentPipe(PixyParentPipe pipe) {
    	this.parentPipe = pipe;
    }

    public T processNextStart() {
		// The pipe doesn't know its ancestors till the first event is received
    	loadPipesIfNecessary();
    	
        this.starts.next();

        for (AsPipe asPipe : this.asPipes) {
            T ans = (T)asPipe.getCurrentEnd();

            if (ans != null) {
            	return ans;
            }
        }

        throw new PixyException(PixyErrorCodes.COALESCE_FAILURE, "Unable to find locate any of the named steps: " + stepNames);
    }

    public void reset() {
        super.reset();
    }

    public String toString() {
    	return PipeHelper.makePipeString(this, this.stepNames.toArray());
    }
}
