package com.example.piglet.jobs;

import java.util.List;
import java.util.Properties;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;


public class JobInfo 
{
	//private Map<String, CounterGroupInfo> counterGroupInfoMap;
	  private List<InputInfo> inputInfoList;
	  private List<OutputInfo> outputInfoList;
	  private long avgMapTime;
	  private long avgReduceTime;
	  private String jobName;
	  private Integer noOfMaps;
	  private Integer noOfReducers;
	  private long bytesWritten;
	  private Properties jobConfProperties;
	  
	  private static final long constreducefactor = 0;
	  private static final long constmapfactor = 0;

	  public JobInfo(Properties jobConfProperties,
	                 List<InputInfo> inputInfoList, List<OutputInfo> outputInfoList,
	                long avgmaptime, long avgreducetime, String jobname, Integer noofmaps,
	                Integer noofreducers, long byteswritten) {
	    this.jobConfProperties = jobConfProperties;
	    this.inputInfoList = inputInfoList;
	    this.outputInfoList = outputInfoList;
	    avgmaptime = (long) Math.ceil((double)(avgmaptime/1000));
	    this.setAvgMapTime((constmapfactor+avgmaptime));
	    avgreducetime = (long) Math.ceil((double)(avgreducetime/1000));
	    this.setAvgReduceTime((constreducefactor+avgreducetime));
	    this.setJobName(jobname);
	    this.setNoOfMaps(noofmaps);
	    this.setNoOfReducers(noofreducers);
	    this.setBytesWritten(byteswritten);
	    //counterGroupInfoMap = CounterGroupInfo.counterGroupInfoMap(jobCounters);
	  }	
	  
	  public List<InputInfo> getInputInfoList() { return inputInfoList; }
	  public List<OutputInfo> getOutputInfoList() { return outputInfoList; }

	  

	  public Properties getJobConfProperties() { return jobConfProperties; }

	  /*public Map<String, CounterGroupInfo> getCounterGroupInfoMap() { return counterGroupInfoMap; }
	  public CounterGroupInfo getCounterGroupInfo(String name) {
	    return counterGroupInfoMap == null ? null : counterGroupInfoMap.get(name);
	  }*/
	  
	  
	  public long getAvgMapTime() {
		return avgMapTime;
	}

	public void setAvgMapTime(long avgMapTime) {
		this.avgMapTime = avgMapTime;
	}

	public long getAvgReduceTime() {
		return avgReduceTime;
	}

	public void setAvgReduceTime(long avgReduceTime) {
		this.avgReduceTime = avgReduceTime;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public Integer getNoOfMaps() {
		return noOfMaps;
	}

	public void setNoOfMaps(Integer noOfMaps) {
		this.noOfMaps = noOfMaps;
	}

	public Integer getNoOfReducers() {
		return noOfReducers;
	}

	public void setNoOfReducers(Integer noOfReducers) {
		this.noOfReducers = noOfReducers;
	}

	public long getBytesWritten() {
		return bytesWritten;
	}

	public void setBytesWritten(long bytesWritten) {
		this.bytesWritten = bytesWritten;
	}

	public static class InputInfo {
		    private String name;
		    private String location;
		    private long numberBytes;
		    private long numberRecords;
		    private boolean successful;
		    private String inputType;

		    @JsonCreator
		    public InputInfo(@JsonProperty("name") String name,
		                     @JsonProperty("location") String location,
		                     @JsonProperty("numberBytes") long numberBytes,
		                     @JsonProperty("numberRecords") long numberRecords,
		                     @JsonProperty("successful") boolean successful,
		                     @JsonProperty("inputType") String inputType) {
		      this.name = name;
		      this.location = location;
		      this.numberBytes = numberBytes;
		      this.numberRecords = numberRecords;
		      this.successful = successful;
		      this.inputType = inputType;
		    }

		    public String getName() { return name; }
		    public String getLocation() { return location; }
		    public long getNumberBytes() { return numberBytes; }
		    public long getNumberRecords() { return numberRecords; }
		    public boolean isSuccessful() { return successful; }
		    public String getInputType() { return inputType; }
		  }
	  
	  public static class OutputInfo {
		    private String name;
		    private String location;
		    private long numberBytes;
		    private long numberRecords;
		    private boolean successful;
		    private String functionName;
		    private String alias;

		    @JsonCreator
		    public OutputInfo(@JsonProperty("name") String name,
		                      @JsonProperty("location") String location,
		                      @JsonProperty("numberBytes") long numberBytes,
		                      @JsonProperty("numberRecords") long numberRecords,
		                      @JsonProperty("successful") boolean successful,
		                      @JsonProperty("functionName") String functionName,
		                      @JsonProperty("alias") String alias) {
		      this.name = name;
		      this.location = location;
		      this.numberBytes = numberBytes;
		      this.numberRecords = numberRecords;
		      this.successful = successful;
		      this.functionName = functionName;
		      this.alias = alias;
		    }

		    public String getName() { return name; }
		    public String getLocation() { return location; }
		    public long getNumberBytes() { return numberBytes; }
		    public long getNumberRecords() { return numberRecords; }
		    public boolean isSuccessful() { return successful; }
		    public String getFunctionName() { return functionName; }
		    public String getAlias() { return alias; }
		  }
	
}
