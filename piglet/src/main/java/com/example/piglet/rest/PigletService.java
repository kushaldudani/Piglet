package com.example.piglet.rest;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.pig.PigRunner;
import org.apache.pig.tools.pigstats.PigStats;

import com.example.piglet.PigletMain;
import com.example.piglet.impl.MRSlotEstimator;
import com.example.piglet.jobs.JobNode;
import com.example.piglet.jobs.MapReduceResult;
import com.example.piglet.listener.PigletListener;

public class PigletService {

  private static final Logger LOGGER = Logger.getLogger(PigletService.class.getName());

  public static MapReduceResult estimateCapacity(String scriptPath, String sla) {

    PigletListener mListener = new PigletListener();

    String[] pigArgs = new String[] { scriptPath };
    PigRunner.run(pigArgs, mListener);
    PigStats.JobGraph jobGraph = PigStats.get().getJobGraph();
    Map<String, JobNode> dagNodeMap = mListener.finalPlan(jobGraph);

    MRSlotEstimator slotEstimator = new MRSlotEstimator();
    LOGGER.info("Received Capacity estimation request with sla : " + sla + " script path = " + scriptPath);

    return slotEstimator.estimateMRSlots(Long.parseLong(sla), dagNodeMap, mListener.getJobInfoList());
  }

}
