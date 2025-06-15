package com.lambdazen.pixy.gremlin;

import com.lambdazen.pixy.PixyErrorCodes;
import com.lambdazen.pixy.PixyException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ScalarMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class PixyCoalesceStep extends ScalarMapStep implements Scoping, TraversalParent, PathProcessor, ByModulating {
    private TraversalRing<Object, Traversal.Admin> traversalRing = new TraversalRing<Object, Traversal.Admin>();
    private final List<String> selectKeys;
    private final Set<String> selectKeysSet;
    private Set<String> keepLabels;

    public PixyCoalesceStep(final Traversal.Admin traversal, final String[] selectKeys) {
        super(traversal);
        this.selectKeys = Arrays.asList(selectKeys);
        this.selectKeysSet = Collections.unmodifiableSet(new HashSet<>(this.selectKeys));
    }

    @Override
    protected Object map(final Traverser.Admin traverser) {
        final Map<String, Object> bindings = new LinkedHashMap<>(this.selectKeys.size(), 1.0f);
        for (final String selectKey : this.selectKeys) {
            final Object end = this.getNullableScopeValue(null, selectKey, traverser);
            if (null != end) {
                bindings.put(selectKey, TraversalUtil.applyNullable(end, this.traversalRing.next()));
            }

            // Commented out of SelectStep by sridhar
            //            else {
            //                this.traversalRing.reset();
            //                return null;
            //            }
        }
        this.traversalRing.reset();

        for (String stepName : selectKeys) {
            Object ans = bindings.get(stepName);
            if (ans != null) {
                return ans;
            }
        }

        throw new PixyException(
                PixyErrorCodes.COALESCE_FAILURE, "Unable to find locate any of the named steps: " + selectKeys);
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.selectKeys, this.traversalRing);
    }

    @Override
    public PixyCoalesceStep clone() {
        final PixyCoalesceStep clone = (PixyCoalesceStep) super.clone();
        clone.traversalRing = this.traversalRing.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin parentTraversal) {
        super.setTraversal(parentTraversal);
        this.traversalRing.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.traversalRing.hashCode() ^ this.selectKeys.hashCode();
        return result;
    }

    @Override
    public List<Traversal.Admin<Object, Traversal.Admin>> getLocalChildren() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> selectTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(selectTraversal));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public Set<String> getScopeKeys() {
        return this.selectKeysSet;
    }

    public Map<String, Traversal.Admin<Object, Traversal.Admin>> getByTraversals() {
        final Map<String, Traversal.Admin<Object, Traversal.Admin>> map = new HashMap<>();
        this.traversalRing.reset();
        for (final String as : this.selectKeys) {
            map.put(as, this.traversalRing.next());
        }
        return map;
    }

    @Override
    public void setKeepLabels(final Set<String> labels) {
        this.keepLabels = labels;
    }

    @Override
    public Set<String> getKeepLabels() {
        return this.keepLabels;
    }

    @Override
    protected Traverser.Admin processNextStart() {
        return PathProcessor.processTraverserPathLabels(super.processNextStart(), this.keepLabels);
    }
}
