package com.example.piglet.impl;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.example.piglet.algo.ExecutionTimeEstimator;
import com.example.piglet.jobs.JobInfo;
import com.example.piglet.jobs.JobNode;
import com.example.piglet.jobs.JobNode.JState;

public class DefaultExecutionTimeEstimator implements ExecutionTimeEstimator {

  private static final Logger LOGGER = Logger.getLogger(DefaultExecutionTimeEstimator.class.getName());

  private Set<Long> timeLine;
  private List<JobNode> jobQueue;
  private Map<String, JobInfo> jobInfoMap;
  private Map<String, JobNode> dagNode;
  private int AMS;
  private int ARS;
  
  private int maxMapatanyTime=0;
  private int maxReduceatanyTime=0;
  
  private long extraMapTime=7;
  private long extraReduceTime=2;

  public DefaultExecutionTimeEstimator(Map<String, JobNode> dagNodeMap, Map<String, JobInfo> jobInfoMap) {
    this.jobInfoMap = jobInfoMap;
    this.dagNode = dagNodeMap;
    jobQueue = new LinkedList<JobNode>();

    timeLine = new TreeSet<Long>();
  }

  @Override
  public long estimateTotalExecTime(int numMapSlots, int numReduceSlots) {
    AMS = numMapSlots;
    ARS = numReduceSlots;

    // Add all the jobs with null parentlist to queue and set their state to RUNNING
    populateQueue();

    Long elaspedtime = new Long(0);
    Long prevelaspedtime;
    do {
      prevelaspedtime = elaspedtime;
      elaspedtime = processQueue(elaspedtime);
    } while (elaspedtime != null);

    return prevelaspedtime;
  }

  private void populateQueue() {
    for (JobNode job : dagNode.values()) {
      if (job.getParents().isEmpty()) {
        job.setJstate(JState.RUNNING);
        job.setStartTime(0);
        jobQueue.add(job);
      }
    }
  }

  private Long processQueue(Long elaspedtime) {
    LOGGER.info("***********processQueue started*************with elasped time " + elaspedtime);
    int cntr = 0;
    while (cntr < jobQueue.size()) {
      JobNode job = jobQueue.get(cntr);
      cntr = updateJobTimeBoundary(job, elaspedtime, cntr);
      cntr++;
    }

    LOGGER.info("Queue--------" + jobQueue);

    // ************************Termination condition**********************************
    if (jobQueue.isEmpty()) {
      return null;
    } else {
      return getElaspedTime();
    }

  }

  private Long getElaspedTime() {
    // Make sure to remove the least value from the timeline and return it

    // ideally this condition is not needed because the termination conditio is taken care in the processQueue function
    // if (!timeLine.iterator().hasNext()) {
    // return null;
    // }

    Long timeBoundary = timeLine.iterator().next();
    timeLine.remove(timeBoundary);
    return timeBoundary;
  }

  private int updateJobTimeBoundary(JobNode job, long elaspedtime, int cntr) {
    LOGGER.info("Job --" + job.getName());
    LOGGER.info("Start AMS---" + AMS);
    LOGGER.info("Start ARS----" + ARS);
    JobInfo jobinfo = jobInfoMap.get(job.getName());

    // Check if the job has completed, elaspedtime == absolute reduce boundary
    if ((job.getElapsedReduceTimeReference() + job.getEstimatedReduceTime()) == elaspedtime) {
      job.setJstate(JState.COMPLETED);
      ARS = ARS + job.getUsingReduceSlots();
      repopulateQueue(job,elaspedtime);
      cntr = -1;
      LOGGER.info("Job Completed");
      LOGGER.info("End AMS---" + AMS);
      LOGGER.info("End ARS----" + ARS);
      return cntr;
    }

    // Check if Map boundary is completed
    if ((job.getElapsedMapTimeReference() + job.getEstimatedMapTime()) == elaspedtime) {
      AMS = AMS + job.getUsingMapSlots();
      job.setMapComplete(true);
      if(job.isOnlyMapJob()){
    	  job.setJstate(JState.COMPLETED);
    	  repopulateQueue(job,elaspedtime);
    	  cntr = -1;
          LOGGER.info("Job Completed");
          LOGGER.info("End AMS---" + AMS);
          LOGGER.info("End ARS----" + ARS);
          return cntr;
      }
    }

    
 // Check if the Map boundary could be updated 
    if((!job.isMapComplete()) && (AMS>0)){
    	if(job.getEstimatedMapTime() == -1){
    		long tmb;
    		if(AMS > jobinfo.getNoOfMaps()){
    			AMS = AMS - jobinfo.getNoOfMaps();
    			job.setUsingMapSlots(jobinfo.getNoOfMaps());
    			tmb = (jobinfo.getAvgMapTime());
    		}else{
    			job.setUsingMapSlots(AMS);
    			AMS=0;
    			tmb = (jobinfo.getAvgMapTime()) * (int)(Math.ceil(((double)(jobinfo.getNoOfMaps())/(double)(job.getUsingMapSlots()))));
    		}
    		tmb = tmb + extraMapTime;
    		job.setEstimatedMapTime(tmb);
    		job.setElapsedMapTimeReference(elaspedtime);
    		timeLine.add((job.getElapsedMapTimeReference()+job.getEstimatedMapTime()));
    	}else{
    		long newtmb;
    		if(job.getUsingMapSlots() < jobinfo.getNoOfMaps()){
    			int newnoofmaps = Math.min(jobinfo.getNoOfMaps(), (AMS+job.getUsingMapSlots()));
    			AMS = AMS - (newnoofmaps - job.getUsingMapSlots());
    			job.setUsingMapSlots(newnoofmaps);
    			newtmb = (jobinfo.getAvgMapTime()) * (int)(Math.ceil(((double)(jobinfo.getNoOfMaps())/(double)(job.getUsingMapSlots()))));
    		}else{
    			newtmb = (jobinfo.getAvgMapTime()) * (int)(Math.ceil(((double)(jobinfo.getNoOfMaps())/(double)(job.getUsingMapSlots()))));
    		}
    		newtmb = newtmb + extraMapTime;
    		long temptm = Math.min(newtmb, (job.getEstimatedMapTime()+job.getElapsedMapTimeReference()-elaspedtime));
    		job.setEstimatedMapTime(temptm);
    		job.setElapsedMapTimeReference(elaspedtime);
    		timeLine.add((job.getElapsedMapTimeReference()+job.getEstimatedMapTime()));
    	}

    }
    
    // Check if the Reduce boundary could be updated 
    if(ARS > 0 && (job.isMapComplete()) && (!job.isOnlyMapJob())){
    	if(job.getEstimatedReduceTime() == -1){
    		long tmb;
    		if(ARS > jobinfo.getNoOfReducers()){
    			ARS =  ARS - jobinfo.getNoOfReducers();
    			job.setUsingReduceSlots(jobinfo.getNoOfReducers());
    			tmb = (jobinfo.getAvgReduceTime());
    		}else{
    			job.setUsingReduceSlots(ARS);
    			ARS=0;
    			tmb = (jobinfo.getAvgReduceTime()) * (int)(Math.ceil(((double)(jobinfo.getNoOfReducers())/(double)(job.getUsingReduceSlots()))));
    		}
    		tmb = tmb+extraReduceTime;
    		if((job.getElapsedMapTimeReference()+job.getEstimatedMapTime()-elaspedtime) > 0){
    			job.setEstimatedReduceTime((job.getElapsedMapTimeReference()+job.getEstimatedMapTime()-elaspedtime+tmb));
    			job.setElapsedReduceTimeReference(elaspedtime);
    		}else{
    			job.setEstimatedReduceTime(tmb);
    			job.setElapsedReduceTimeReference(elaspedtime);
    		}
    		timeLine.add((job.getEstimatedReduceTime()+job.getElapsedReduceTimeReference()));
    	}else{
    		long newtmb;
    		if(job.getUsingReduceSlots() < jobinfo.getNoOfReducers()){
    			int newnoofreducers = Math.min(jobinfo.getNoOfReducers(), (ARS+job.getUsingReduceSlots()));
    			ARS = ARS - (newnoofreducers - job.getUsingReduceSlots());
    			job.setUsingReduceSlots(newnoofreducers);
    			newtmb = (jobinfo.getAvgReduceTime()) * (int)(Math.ceil(((double)(jobinfo.getNoOfReducers())/(double)(job.getUsingReduceSlots()))));
    		}else{
    			newtmb = (jobinfo.getAvgReduceTime()) * (int)(Math.ceil(((double)(jobinfo.getNoOfReducers())/(double)(job.getUsingReduceSlots()))));
    		}
    		newtmb = newtmb+extraReduceTime;
    		if((job.getElapsedMapTimeReference()+job.getEstimatedMapTime()-elaspedtime) > 0){
    			job.setEstimatedReduceTime((job.getElapsedMapTimeReference()+job.getEstimatedMapTime()-elaspedtime+newtmb));
    			job.setElapsedReduceTimeReference(elaspedtime);
    		}else{
    			long temptm = Math.min(newtmb, (job.getEstimatedReduceTime()+job.getElapsedReduceTimeReference()-elaspedtime));
    			job.setEstimatedReduceTime(temptm);
    			job.setElapsedReduceTimeReference(elaspedtime);
    		}
    		timeLine.add((job.getEstimatedReduceTime()+job.getElapsedReduceTimeReference()));
    	}

    }

    LOGGER.info("End AMS---" + AMS);
    LOGGER.info("End ARS----" + ARS);
    LOGGER.info("estimated Map Time" + job.getEstimatedMapTime());
    LOGGER.info("reference Map Time" + job.getElapsedMapTimeReference());
    LOGGER.info("estimated reduce Time" + job.getEstimatedReduceTime());
    LOGGER.info("reference reduce Time" + job.getElapsedReduceTimeReference());
    return cntr;
  }

  private void repopulateQueue(JobNode job, long elaspedtime) {
    LOGGER.info("Repopulate Queue-------------");
    findMaxMapReduceAtAnyTime();
    job.setEndTime(elaspedtime);
    // remove that job from the queue
    jobQueue.remove(job);

    // add new job by checking the predecessor if they are all completed
    for (JobNode newJob : dagNode.values()) {
      if (JobNode.JState.INITTED == newJob.getJstate()) {

        boolean allParentsCompleted = false;
        for (JobNode parent : newJob.getParents()) {
          if (parent.getJstate() == JState.COMPLETED) {
            allParentsCompleted = true;
          } else {
            allParentsCompleted = false;
          }
        }
        if (allParentsCompleted) {
          LOGGER.info(newJob.getName());
          newJob.setJstate(JState.RUNNING);
          newJob.setStartTime(elaspedtime);
          jobQueue.add(newJob);
        }
      }
    }

  }
  
  private void findMaxMapReduceAtAnyTime(){
	  int mMap=0;
	  int mReduce=0;
	  for(JobNode node : jobQueue){
		  JobInfo jobinfo = jobInfoMap.get(node.getName());
		  mMap = mMap+jobinfo.getNoOfMaps();
		  mReduce = mReduce+jobinfo.getNoOfReducers();
	  }
	  
	  if(maxMapatanyTime < mMap){
		  maxMapatanyTime = mMap;
	  }
	  if(maxReduceatanyTime < mReduce){
		  maxReduceatanyTime = mReduce;
	  }
  }
  
  public int getmaxMapatanyTime(){
	  return maxMapatanyTime;
  }
  
  public int getmaxReduceatanyTime(){
	  return maxReduceatanyTime;
  }

}
