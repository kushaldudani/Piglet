package com.example.piglet.jobs;

import java.util.ArrayList;
import java.util.List;

public class MapReduceResult {

  private int numMapSlots;
  private int numReduceSlots;
  private long diskSpace;
  private long execTime;
  
  public static class JobStats {
    private int numMapSlots;
    private int numReduceSlots;
    private long startTimeSecs;
    private long finishTimeSecs;
    private String jobId;
    
    public String getJobId() {
      return jobId;
    }
    public void setJobId(String jobId) {
      this.jobId = jobId;
    }
    public int getNumMapSlots() {
      return numMapSlots;
    }
    public void setNumMapSlots(int numMapSlots) {
      this.numMapSlots = numMapSlots;
    }
    public int getNumReduceSlots() {
      return numReduceSlots;
    }
    public void setNumReduceSlots(int numReduceSlots) {
      this.numReduceSlots = numReduceSlots;
    }
    public long getStartTimeSecs() {
      return startTimeSecs;
    }
    public void setStartTimeSecs(long startTimeSecs) {
      this.startTimeSecs = startTimeSecs;
    }
    public long getFinishTimeSecs() {
      return finishTimeSecs;
    }
    public void setFinishTimeSecs(long finishTimeSecs) {
      this.finishTimeSecs = finishTimeSecs;
    }
  }
  
  private List<JobStats> iterationStats;
  

  public List<JobStats> getIndividualJobsStats() {
    return iterationStats;
  }

  public void setIndividualJobsStats(List<JobStats> iterationStats) {
    this.iterationStats = iterationStats;
  }

  public int getNumMapSlots() {
    return numMapSlots;
  }

  public void setNumMapSlots(int numMapSlots) {
    this.numMapSlots = numMapSlots;
  }

  public int getNumReduceSlots() {
    return numReduceSlots;
  }

  public void setNumReduceSlots(int numReduceSlots) {
    this.numReduceSlots = numReduceSlots;
  }

  public long getDiskSpace() {
    return diskSpace;
  }

  public void setDiskSpace(long diskSpace) {
    this.diskSpace = diskSpace;
  }

public long getExecTime() {
	return execTime;
}

public void setExecTime(long execTime) {
	this.execTime = execTime;
}

@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Maps-");
		sb.append(numMapSlots);
		sb.append("Reduce-");
		sb.append(numReduceSlots);
		sb.append("execTime-");
		sb.append(execTime);
		
		return sb.toString();
	}



}
