package com.lambdazen.pixy.pipes;

public class LoopDetails {
	private String loopVarName;
	private int minLoops;
	private int maxLoops;

	public LoopDetails(String loopVarName, int minLoops, int maxLoops) {
		this.loopVarName = loopVarName;
		this.minLoops = minLoops;
		this.maxLoops = maxLoops;
	}

	public String getLoopVarName() {
		return loopVarName;
	}

	public int getMinLoops() {
		return minLoops;
	}

	public int getMaxLoops() {
		return maxLoops;
	}
}