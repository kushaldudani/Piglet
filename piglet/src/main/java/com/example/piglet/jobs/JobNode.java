package com.example.piglet.jobs;

import java.util.Collection;
import java.util.List;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;


public class JobNode {
	
	  private String name;
	  private String aliases;
	  private String features;
	  private String jobId;
	  private List<JobNode> predecessorNodeList;
	  //private Collection<String> predecessorNames;
//	  private Integer dagLevel;
	  private JState jstate = JState.INITTED;
	  private long estimatedMapTime=-1;
	  private long estimatedReduceTime=-1;
	  private long elapsedMapTimeReference=-1;
	  private long elapsedReduceTimeReference=-1;
	  
	  private int usingMapSlots=0;
	  private int usingReduceSlots=0;
	  private boolean mapComplete=false;
	  
	  private boolean onlyMapJob=false;
	  private long startTime=-1;
	  private long endTime=-1;
	  
	  public enum JState {
	     RUNNING, COMPLETED, INITTED
	  }
	  
	  private Double x, y;
	  
	  public JobNode(String name, String aliases, String features) {
		    this.name = name;
		    this.aliases = aliases;
		    this.features = features;
		  }

		  @JsonCreator
		  public JobNode(@JsonProperty("name") String name,
		                 @JsonProperty("aliases") String aliases,
		                 @JsonProperty("features") String features,
		                 @JsonProperty("jobId") String jobId,
		                 @JsonProperty("successorNames") Collection<String> predecessorNames,
		                 @JsonProperty("runtime") String runtime) {
		    this.name = name;
		    this.aliases = aliases;
		    this.features = features;
		    this.jobId = jobId;
		    //this.predecessorNames = predecessorNames;
		  }
		  
		  public String getName() { return name; }
		  public String getAliases() { return aliases;  }
		  public String getFeatures() { return features; }


	 public void reinitializeNode(){
		 jstate = JState.INITTED;
		  estimatedMapTime=-1;
		  estimatedReduceTime=-1;
		  elapsedMapTimeReference=-1;
		  elapsedReduceTimeReference=-1;
		  
		  usingMapSlots=0;
		  usingReduceSlots=0;
		  mapComplete=false;
		  
		  startTime = -1;
		  endTime = -1;
	 }
		  
      public long getEstimatedMapTime() {
        return estimatedMapTime;
      }

      public void setEstimatedMapTime(long estimatedMapTime) {
        this.estimatedMapTime = estimatedMapTime;
      }

      public long getEstimatedReduceTime() {
        return estimatedReduceTime;
      }

      public void setEstimatedReduceTime(long estimatedReduceTime) {
        this.estimatedReduceTime = estimatedReduceTime;
      }

      public long getElapsedMapTimeReference() {
        return elapsedMapTimeReference;
      }

      public void setElapsedMapTimeReference(long elapsedMapTimeReference) {
        this.elapsedMapTimeReference = elapsedMapTimeReference;
      }

      public long getElapsedReduceTimeReference() {
        return elapsedReduceTimeReference;
      }

      public void setElapsedReduceTimeReference(long elapsedReduceTimeReference) {
        this.elapsedReduceTimeReference = elapsedReduceTimeReference;
      }

      public void setName(String name) {
        this.name = name;
      }

      public void setFeatures(String features) {
        this.features = features;
      }

      public String getJobId() { return jobId; }
		  public void setJobId(String jobId) { this.jobId = jobId; }

//		  public Integer getDagLevel() { return dagLevel; }
//		  public void setDagLevel(Integer dagLevel) { this.dagLevel = dagLevel; }

		  public Double getX() { return x; }
		  public void setX(Double x) { this.x = x; }

		  public Double getY() { return y; }
		  public void setY(Double y) { this.y = y; }
		  
		  @JsonIgnore
		  public synchronized List<JobNode> getParents() { return predecessorNodeList ;}
		  public synchronized void setParents(List<JobNode> predecessorNodeList) {
		    //Collection<String> predecessorNames = new HashSet<String>();
		    //if (predecessorNodeList != null) {
		     // for(JobNode node : predecessorNodeList) {
		     //   predecessorNames.add(node.getName());
		     // }
		    //}
		    this.predecessorNodeList = predecessorNodeList;
		    //this.predecessorNames = predecessorNames;
		  }

		  //public synchronized Collection<String> getPredecessorNames() { return predecessorNames; }

		public JState getJstate() {
			return jstate;
		}

		public void setJstate(JState jstate) {
			this.jstate = jstate;
		}

		public int getUsingMapSlots() {
			return usingMapSlots;
		}

		public void setUsingMapSlots(int usingMapSlots) {
			this.usingMapSlots = usingMapSlots;
		}

		public int getUsingReduceSlots() {
			return usingReduceSlots;
		}

		public void setUsingReduceSlots(int usingReduceSlots) {
			this.usingReduceSlots = usingReduceSlots;
		}

		public boolean isMapComplete() {
			return mapComplete;
		}

		public void setMapComplete(boolean mapComplete) {
			this.mapComplete = mapComplete;
		}

		public boolean isOnlyMapJob() {
			return onlyMapJob;
		}

		public void setOnlyMapJob(boolean onlyMapJob) {
			this.onlyMapJob = onlyMapJob;
		}

		public long getStartTime() {
			return startTime;
		}

		public void setStartTime(long startTime) {
			this.startTime = startTime;
		}

		public long getEndTime() {
			return endTime;
		}

		public void setEndTime(long endTime) {
			this.endTime = endTime;
		}

}
