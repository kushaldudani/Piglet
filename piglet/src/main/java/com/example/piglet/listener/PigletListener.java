package com.example.piglet.listener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.newplan.Operator;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;

import com.example.piglet.jobs.JobInfo;
import com.example.piglet.jobs.JobNode;
import com.example.piglet.jobs.PigJobs;



public class PigletListener implements PigProgressNotificationListener {
	
	protected Log log = LogFactory.getLog(getClass());
	private Map<String,JobInfo> jobInfoList = new HashMap<String,JobInfo>();
	/*private Map<String, JobNode> dagNodeNameMapInitial = new TreeMap<String, JobNode>();*/
	private Map<String, JobNode> dagNodeNameMap = new LinkedHashMap<String, JobNode>();
	/*private HashSet<String> completedJobIds = new HashSet<String>();*/
	
	public PigletListener(){
		
	}
	
	public Map<String,JobInfo> getJobInfoList(){
		return jobInfoList;
	}
	
	/*public void initialPlanNotification(MROperPlan plan) {
		Map<OperatorKey, MapReduceOper>  planKeys = plan.getKeys();
		for (Map.Entry<OperatorKey, MapReduceOper> entry : planKeys.entrySet()) {
		      JobNode node = new JobNode(entry.getKey().toString(),
		        null,
		        null,"pig");

		      dagNodeNameMapInitial.put(node.getName(), node);

		      
		      System.out.println("initialPlanNotification: " + "name: " + node.getName() );
		    }
		
		for (Map.Entry<OperatorKey, MapReduceOper> entry : planKeys.entrySet()) {
		      JobNode node = dagNodeNameMapInitial.get(entry.getKey().toString());
		      List<JobNode> successorNodeList = new ArrayList<JobNode>();
		      List<MapReduceOper> successors = plan.getSuccessors(entry.getValue());

		      if (successors != null) {
		        for (MapReduceOper successor : successors) {
		          JobNode successorNode = dagNodeNameMapInitial.get(successor.getOperatorKey().toString());
		          successorNodeList.add(successorNode);
		        }
		      }

		      node.setSuccessors(successorNodeList);
		    }
		
	}*/
	
	public Map<String, JobNode> finalPlan(JobGraph jobGraph){
		Iterator<Operator> itr = jobGraph.getOperators();
		
		System.out.println("----------------------------------"+jobGraph.toString());
		while(itr.hasNext()) {
			JobStats job = (JobStats) itr.next();
			JobNode node = new JobNode(job.getName(),job.getAlias(),job.getFeature());
			dagNodeNameMap.put(node.getName(), node);
			
			JobInfo jobinfo = jobInfoList.get(job.getName());
			System.out.println("finalPlanNotification: " + "name: " + job.getName() + " id: "+job.getJobId() + " avgMapTime: "+
			jobinfo.getAvgMapTime() + " avgReduceTime: "+ jobinfo.getAvgReduceTime());
			
			if(jobinfo.getNoOfReducers()==0){
				node.setOnlyMapJob(true);
			}
		
		}
		Iterator<Operator> itr1 = jobGraph.getOperators();
		while(itr1.hasNext()){
			Operator job = itr1.next();
			JobNode node = dagNodeNameMap.get(job.getName());
			List<JobNode> predecessorNodeList = new ArrayList<JobNode>();
			List<Operator> parents = jobGraph.getPredecessors(job);
			
			if (parents != null) {
				for(Operator pn : parents){
				  predecessorNodeList.add(dagNodeNameMap.get(pn.getName()));
				}
			}
			node.setParents(predecessorNodeList);
		}
		return dagNodeNameMap;
	}

	@Override
	public void jobFailedNotification(JobStats arg0) {
		System.out.println("------------------"+arg0.getJobId());
		System.out.println("--------------------"+arg0.getName());
		JobInfo jobinfo = collectStats(arg0);
		jobInfoList.put(jobinfo.getJobName(),jobinfo);
		
	}

	@Override
	public void jobFinishedNotification(JobStats arg0) {
		System.out.println("------------------"+arg0.getJobId());
		System.out.println("--------------------"+arg0.getName());
		JobInfo jobinfo = collectStats(arg0);
		jobInfoList.put(jobinfo.getJobName(), jobinfo);
		
	}

	@Override
	public void jobStartedNotification(String assignedJobId) {
		/*PigStats.JobGraph jobGraph = PigStats.get().getJobGraph();
	    System.out.println("jobStartedNotification - jobId " + assignedJobId + ", jobGraph:\n" + jobGraph);
	    
	    for (JobStats jobStats : jobGraph) {
	    	if (assignedJobId.equals(jobStats.getJobId())) {
	    		System.out.println("jobStartedNotification - scope " + jobStats.getName() + " is jobId " + assignedJobId);
	    		JobNode node = this.dagNodeNameMapInitial.get(jobStats.getName());
	    		if (node == null) {
	    	          System.out.println("jobStartedNotification - unrecorgnized operator name found ("
	    	                  + jobStats.getName() + ") for jobId " + assignedJobId);
	    	        } else {
	    	        	System.out.println("jobid and jobname matched");
	    	        	node.setJobId(assignedJobId);
	    	        }
	    	}
	    }*/
		
	}

	@Override
	public void jobsSubmittedNotification(int arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void launchCompletedNotification(int arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void launchStartedNotification(int arg0) {
		
	}

	@Override
	public void outputCompletedNotification(OutputStats arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void progressUpdatedNotification(int arg0) {
		// TODO Auto-generated method stub
		
	}
	
	private JobInfo collectStats(JobStats stats){
		Properties jobConfProperties = new Properties();
		if(stats.getInputs() != null && stats.getInputs().size() > 0 && stats.getInputs().get(0).getConf() !=null){
			Configuration conf = stats.getInputs().get(0).getConf();
			for(Map.Entry<String, String> entry : conf){
				jobConfProperties.setProperty(entry.getKey(), entry.getValue());
			}
		}
		
		return new PigJobs(stats,jobConfProperties);
	}
	
	
	private static String[] toArray(String string) {
	    return string == null ? new String[0] : string.trim().split(",");
	  }
	
	private static String toString(String[] array) {
	    StringBuilder sb = new StringBuilder();
	    for (String string : array) {
	      if (sb.length() > 0) { sb.append(","); }
	      sb.append(string);
	    }
	    return sb.toString();
	  }

}
