package com.lambdazen.pixy.gremlin;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.AbstractMetaPipe;
import com.tinkerpop.pipes.util.AsPipe;
import com.tinkerpop.pipes.util.FluentUtility;
import com.tinkerpop.pipes.util.MetaPipe;
import com.tinkerpop.pipes.util.PipeHelper;
import com.tinkerpop.pipes.util.Pipeline;

import java.util.ArrayList;
import java.util.List;

public class PixySplitMergePipe<S, E> extends AbstractMetaPipe<S, E> implements MetaPipe, PixyParentPipe, PixyParentQueryPipe {
    public static final String PIXY_CUT_PREFIX = "__pixy_cut_";
    
	private final List<Pipeline> pipelines;
    private final List<AsPipe> asPipeList;
    private final List<List<AsPipe>> cutPipeList;
    private int numPipelines;
    private int current;
    private List<S> nextStart;
    private PixyParentPipe parentPipe; 

    public PixySplitMergePipe(List<Pipeline> pipelines, List<AsPipe> asList) {
        this.pipelines = pipelines;
        this.numPipelines = pipelines.size();
        this.cutPipeList = new ArrayList<List<AsPipe>>(numPipelines);
        this.current = 0;
        this.nextStart = new ArrayList<S>();
        this.parentPipe = null;
        this.asPipeList = asList;
        
        for (Pipeline pipeline : pipelines) {
        	setParent(pipeline);
        }

        for (Pipeline pipeline : pipelines) {
        	List<AsPipe> cutPipes = null;
        	//for (AsPipe asPipe: FluentUtility.getAsPipes(pipeline)) {
    		List<Pipe> pipes = pipeline.getPipes();
    		for (Pipe pipe: pipes) {
        		if (!(pipe instanceof AsPipe)) {
        			continue;
        		}

        		AsPipe asPipe = (AsPipe)pipe;
        		if (asPipe.getName().startsWith(PIXY_CUT_PREFIX)) {
        			// This is a cut -- need to track it
        			if (cutPipes == null) {
        				cutPipes = new ArrayList<AsPipe>();
        			}
        			
        			cutPipes.add(asPipe);
        		}
            }
        	
        	cutPipeList.add(cutPipes);
        }
    }

	protected void setParent(MetaPipe pipeline) {
		List<Pipe> pipes = pipeline.getPipes();
		for (Pipe pipe : pipes) {
			if (pipe instanceof PixyParentQueryPipe) {
				((PixyParentQueryPipe)pipe).setParentPipe(this);
			} else if (pipe instanceof MetaPipe) {
				// Some pipes like AsPipe are MetaPipe's that hold coalesce, eval, etc. 
				setParent((MetaPipe)pipe);
			}
		}
	}

    public E processNextStart() {
    	if (nextStart.size() == 0) {
	    	// This is the first call to processNextStart()
    		
    		// Get the first start
	    	this.nextStart.add(starts.next());
	    	
	    	// Feed the first pipe
	        this.pipelines.get(0).setStarts(nextStart);
    	}

    	// Go over the pipes in order and extract values
        while (true) {
        	Pipe pipe = this.pipelines.get(this.current);
        	List<AsPipe> cutPipes = this.cutPipeList.get(this.current);

        	if (pipe.hasNext()) {
            	// The current pipe has another element
                return (E) pipe.next();
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
                    this.nextStart.clear();
                    
                    // This may throw NoSuchElementException to indicate the end of processing
                    this.nextStart.add(starts.next());
                }

                assert (nextStart != null);
                
                // Feed the nextStart to the current pipe
                this.pipelines.get(this.current).setStarts(nextStart);
            }
        }
    }

    private boolean wasCut(List<AsPipe> cutPipes) {
    	if (cutPipes == null) {
    		return false;
    	}

    	for (AsPipe asPipe : cutPipes) {
    		assert asPipe.getPipes().size() > 0;
    		assert asPipe.getPipes().get(0) instanceof PixyCutPipe;

			if (((PixyCutPipe)(asPipe.getPipes().get(0))).wasCut()) {
				return true;
			}
		}

		return false;
	}

	public List<Pipe> getPipes() {
    	List<Pipe> ans = new ArrayList<Pipe>();
    	for (Pipeline pipe : pipelines) {
    		ans.add(pipe);
    	}

    	return ans;
    }

    public String toString() {
        return PipeHelper.makePipeString(this, this.pipelines);
    }
    
    @Override
    public void reset() {
        super.reset();

        this.current = 0;
        this.nextStart.clear();
    }

	@Override
	public List<AsPipe> getAsPipes() {
		if (parentPipe == null) {
			return this.asPipeList;			
		} else {
			//assert 1 == 2;
			List<AsPipe> ans = new ArrayList<AsPipe>(asPipeList);
			ans.addAll(parentPipe.getAsPipes());
			return ans;
		}
	}

	@Override
	public void setParentPipe(PixyParentPipe pipe) {
		this.parentPipe = pipe;
	}
}