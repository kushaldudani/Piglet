package com.example.piglet.jobs;

import java.io.IOException;
import java.util.List;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;




public class WorkFlowInfo {
	
	private String workflowId;
	  private String workflowFingerprint;
	  private List<JobInfo> jobInfoList;
	  
	  @JsonCreator
	  public WorkFlowInfo(@JsonProperty("workflowId") String workflowId,
	                      @JsonProperty("workflowFingerprint") String workflowFingerprint,
	                      @JsonProperty("jobInfoList") List<JobInfo> jobInfoList) {
	    this.workflowId = workflowId;
	    this.workflowFingerprint = workflowFingerprint;
	    this.jobInfoList = jobInfoList;
	  }

	  public String getWorkflowId() { return workflowId; }
	  public String getWorkflowFingerprint() { return workflowFingerprint; }
	  public List<JobInfo> getJobInfoList() { return jobInfoList; }
	  
	  
	  public static String toJSON(WorkFlowInfo workflowInfo) throws IOException {
		    ObjectMapper om = new ObjectMapper();
		    om.getSerializationConfig().set(SerializationConfig.Feature.INDENT_OUTPUT, true);
		    return om.writeValueAsString(workflowInfo);
		  }
	  
	  public static WorkFlowInfo fromJSON(String workflowInfoJson) throws IOException {
		    ObjectMapper om = new ObjectMapper();
		    om.getDeserializationConfig().set(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		    return om.readValue(workflowInfoJson, WorkFlowInfo.class);
		  }

}
