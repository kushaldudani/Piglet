package com.example.piglet.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.example.piglet.PigletMain;
import com.example.piglet.jobs.JobInfo;
import com.example.piglet.jobs.JobNode;
import com.example.piglet.jobs.MapReduceResult;
import com.example.piglet.jobs.MapReduceResult.JobStats;

public class MRSlotEstimator {

  private MapReduceResult mrResult = new MapReduceResult();
  private static final Logger LOGGER = Logger.getLogger(PigletMain.class.getName());

  public MapReduceResult estimateMRSlots(long SLA, Map<String, JobNode> dagNodeMap, Map<String, JobInfo> jobInfoMap) {
    System.out.println("******************************************estimateMRSlots Started************************************");
    // TODO - make sla conf
    int MAXMSLOT = 0;
    int MAXRSLOT = 0;

    for (JobNode node : dagNodeMap.values()) {
      JobInfo job = jobInfoMap.get(node.getName());
      MAXMSLOT = MAXMSLOT + job.getNoOfMaps();
      MAXRSLOT = MAXRSLOT + job.getNoOfReducers();
    }
    reInitializeDAG(dagNodeMap);
    DefaultExecutionTimeEstimator timeEstimator = new DefaultExecutionTimeEstimator(dagNodeMap, jobInfoMap);
    timeEstimator.estimateTotalExecTime(MAXMSLOT, MAXRSLOT);
    MAXMSLOT = timeEstimator.getmaxMapatanyTime();
    MAXRSLOT = timeEstimator.getmaxReduceatanyTime();
    // check if the SLA is too low that with even
    if (checkIfExceedsSLA(dagNodeMap, jobInfoMap, MAXMSLOT, MAXRSLOT, SLA)) {
      System.err.println("Total Exec Time with MAXMSLOT and MAXRSLOT exceeds SLA " + SLA);
      return null;
    }

    int[] optimalSlots = getOptimalMapReduceSlots(MAXMSLOT, MAXRSLOT);
    int OMSLOT = optimalSlots[0];
    int ORSLOT = optimalSlots[1];
    boolean isMapHeavy = (optimalSlots[2] == 1) ? true : false;

    if (checkIfExceedsSLA(dagNodeMap, jobInfoMap, OMSLOT, ORSLOT, SLA)) {
      if (isMapHeavy) {
        mrResult = searchVaryingMapSlots(dagNodeMap, jobInfoMap, ORSLOT, OMSLOT, MAXMSLOT, SLA);
      } else {
        mrResult = searchVaryingReduceSlots(dagNodeMap, jobInfoMap, OMSLOT, ORSLOT, MAXRSLOT, SLA);
      }
    } else {
      mrResult = binarySearchMRSlots(dagNodeMap, jobInfoMap, OMSLOT, ORSLOT, SLA);
    }

    return mrResult;
  }

  private MapReduceResult searchVaryingReduceSlots(Map<String, JobNode> dagNodeMap, Map<String, JobInfo> jobInfoMap, int oMSLOT, int oRSLOT,
      int mAXRSLOT, long SLA) {
    System.out.println("*******************************It came to searchVaryingReduceSlots***********************");
    long execTime = 0;
    DefaultExecutionTimeEstimator timeEstimator;

    int lowReduceSlots = oRSLOT;
    int highReduceSlots = mAXRSLOT;

    int currentReduceSlots = highReduceSlots;

    do {
      if (execTime > SLA) {
        lowReduceSlots = currentReduceSlots;
        currentReduceSlots = (int) (Math.ceil(((double) (lowReduceSlots + highReduceSlots) / (double) 2)));
      } else {
        highReduceSlots = currentReduceSlots;
        currentReduceSlots = (int) (Math.ceil(((double) (lowReduceSlots + highReduceSlots) / (double) 2)));
      }

      reInitializeDAG(dagNodeMap);
      timeEstimator = new DefaultExecutionTimeEstimator(dagNodeMap, jobInfoMap);
      execTime = timeEstimator.estimateTotalExecTime(oMSLOT, currentReduceSlots);

      System.out.println("MSLOT -" + oMSLOT);
      System.out.println("RSLOT -" + currentReduceSlots);
      System.out.println("execTime -" + execTime);

    } while ((!isNearSLA(execTime, SLA)) && (!isSlotsEqual(currentReduceSlots, highReduceSlots)));

    mrResult.setIndividualJobsStats(getJobStats(dagNodeMap));
    mrResult.setNumMapSlots(oMSLOT);
    mrResult.setNumReduceSlots(currentReduceSlots);
    mrResult.setExecTime(execTime);

    return mrResult;
  }

  private MapReduceResult searchVaryingMapSlots(Map<String, JobNode> dagNodeMap, Map<String, JobInfo> jobInfoMap, int oRSLOT, int oMSLOT,
      int mAXMSLOT, long SLA) {
    System.out.println("***************************It came to searchVaryingMapSlots*******************************");
    long execTime = 0;
    DefaultExecutionTimeEstimator timeEstimator;

    int lowMapSlots = oMSLOT;
    int highMapSlots = mAXMSLOT;

    int currentMapSlots = highMapSlots;

    do {
      if (execTime > SLA) {
        lowMapSlots = currentMapSlots;
        currentMapSlots = (int) (Math.ceil(((double) (lowMapSlots + highMapSlots) / (double) 2)));
      } else {
        highMapSlots = currentMapSlots;
        currentMapSlots = (int) (Math.ceil(((double) (lowMapSlots + highMapSlots) / (double) 2)));
      }

      reInitializeDAG(dagNodeMap);
      timeEstimator = new DefaultExecutionTimeEstimator(dagNodeMap, jobInfoMap);
      execTime = timeEstimator.estimateTotalExecTime(currentMapSlots, oRSLOT);

      System.out.println("MSLOT -" + currentMapSlots);
      System.out.println("RSLOT -" + oRSLOT);
      System.out.println("execTime -" + execTime);

    } while ((!isNearSLA(execTime, SLA)) && (!isSlotsEqual(currentMapSlots, highMapSlots)));

    mrResult.setIndividualJobsStats(getJobStats(dagNodeMap));
    mrResult.setNumMapSlots(currentMapSlots);
    mrResult.setNumReduceSlots(oRSLOT);
    mrResult.setExecTime(execTime);

    return mrResult;
  }

  private MapReduceResult binarySearchMRSlots(Map<String, JobNode> dagNodeMap, Map<String, JobInfo> jobInfoMap, int oMSLOT, int oRSLOT, long SLA) {
    System.out.println("************************************It came to binarySearchMRSlots****************************");
    long execTime = 0;
    DefaultExecutionTimeEstimator timeEstimator;

    int lowMapSlots = 1;
    int lowReduceSlots = 1;
    int highMapSlots = oMSLOT;
    int highReduceSlots = oRSLOT;

    int currentMapSlots = highMapSlots;
    int currentReduceSlots = highReduceSlots;

    do {
      if (execTime > SLA) {
        if ((highMapSlots - lowMapSlots) > 1) {
          lowMapSlots = currentMapSlots;
        }
        if ((highReduceSlots - lowReduceSlots) > 1) {
          lowReduceSlots = currentReduceSlots;
        }
        currentMapSlots = (int) (Math.ceil(((double) (lowMapSlots + highMapSlots) / (double) 2)));
        currentReduceSlots = (int) (Math.ceil(((double) (lowReduceSlots + highReduceSlots) / (double) 2)));
      } else {
        if ((highMapSlots - lowMapSlots) > 1) {
          highMapSlots = currentMapSlots;
        }
        if ((highReduceSlots - lowReduceSlots) > 1) {
          highReduceSlots = currentReduceSlots;
        }
        currentMapSlots = (int) (Math.floor(((double) (lowMapSlots + highMapSlots) / (double) 2)));
        currentReduceSlots = (int) (Math.floor(((double) (lowReduceSlots + highReduceSlots) / (double) 2)));
      }

      reInitializeDAG(dagNodeMap);
      timeEstimator = new DefaultExecutionTimeEstimator(dagNodeMap, jobInfoMap);
      execTime = timeEstimator.estimateTotalExecTime(currentMapSlots, currentReduceSlots);

      System.out.println("MSLOT -" + currentMapSlots);
      System.out.println("RSLOT -" + currentReduceSlots);
      System.out.println("execTime -" + execTime);

      if (isNearSLA(execTime, SLA)) {
        mrResult.setIndividualJobsStats(getJobStats(dagNodeMap));
        mrResult.setNumMapSlots(currentMapSlots);
        mrResult.setNumReduceSlots(currentReduceSlots);
        mrResult.setExecTime(execTime);
        break;
      }

      if (((highMapSlots - lowMapSlots) <= 1) && ((highReduceSlots - lowReduceSlots) <= 1)) {
        int[] mSlots = { lowMapSlots, highMapSlots };
        int[] rSlots = { lowReduceSlots, highReduceSlots };
        mrResult = terminateBinarySearch(dagNodeMap, jobInfoMap, mSlots, rSlots, SLA);
        break;
      }

    } while (true);

    return mrResult;
  }

  private MapReduceResult terminateBinarySearch(Map<String, JobNode> dagNodeMap, Map<String, JobInfo> jobInfoMap, int[] mSlots, int[] rSlots,
      long SLA) {
    System.out.println("************************************It came to terminateBinarySearch****************************");
    long execTime = 0;
    long time;
    DefaultExecutionTimeEstimator timeEstimator;

    for (int mcntr = 0; mcntr < 2; mcntr++) {
      for (int rcntr = 0; rcntr < 2; rcntr++) {
        reInitializeDAG(dagNodeMap);
        timeEstimator = new DefaultExecutionTimeEstimator(dagNodeMap, jobInfoMap);
        time = timeEstimator.estimateTotalExecTime(mSlots[mcntr], rSlots[rcntr]);
        if ((execTime < time) && (time <= SLA)) {
          execTime = time;
          mrResult.setIndividualJobsStats(getJobStats(dagNodeMap));
          mrResult.setExecTime(execTime);
          mrResult.setNumMapSlots(mSlots[mcntr]);
          mrResult.setNumReduceSlots(rSlots[rcntr]);
        }
      }
    }
    return mrResult;
  }

  private boolean checkIfExceedsSLA(Map<String, JobNode> dagNodeMap, Map<String, JobInfo> jobInfoMap, int MSLOT, int RSLOT, long SLA) {
    reInitializeDAG(dagNodeMap);
    DefaultExecutionTimeEstimator timeEstimator = new DefaultExecutionTimeEstimator(dagNodeMap, jobInfoMap);
    long totalExecTime = timeEstimator.estimateTotalExecTime(MSLOT, RSLOT);
    if (totalExecTime > SLA) {
      return true;
    }
    return false;
  }

  private boolean isSlotsEqual(int curMorRSlots, int highMorRSlots) {
    if (curMorRSlots == highMorRSlots) {
      return true;
    } else {
      return false;
    }
  }

  private boolean isNearSLA(long totalExecTime, long SLA) {
    if ((totalExecTime >= (0.9 * SLA)) && (totalExecTime <= SLA)) {
      return true;
    }
    return false;
  }

  private int[] getOptimalMapReduceSlots(int mapSlots, int reduceSlots) {
    int numMapSlots;
    int numReduceSlots;
    int mapHeavy;
    // map heavy
    if (mapSlots >= (4 * reduceSlots)) {
      numReduceSlots = reduceSlots;
      numMapSlots = (4 * reduceSlots);
      mapHeavy = 1;
    } else {
      numMapSlots = mapSlots;
      numReduceSlots = (int) (Math.ceil(((double) (numMapSlots) / (double) 4)));
      mapHeavy = 0;
    }

    return new int[] { numMapSlots, numReduceSlots, mapHeavy };
  }

  private void reInitializeDAG(Map<String, JobNode> dagNodeMap) {
    for (JobNode node : dagNodeMap.values()) {
      node.reinitializeNode();
    }
  }

  private List<JobStats> getJobStats(Map<String, JobNode> dagNodeMap) {

    List<MapReduceResult.JobStats> jobIds = new ArrayList<MapReduceResult.JobStats>();
    for (JobNode node : dagNodeMap.values()) {
      MapReduceResult.JobStats jobStats = new MapReduceResult.JobStats();
      jobStats.setNumMapSlots(node.getUsingMapSlots());
      jobStats.setNumReduceSlots(node.getUsingReduceSlots());
      jobStats.setFinishTimeSecs(node.getEndTime());
      jobStats.setStartTimeSecs(node.getStartTime());
      jobStats.setJobId(node.getName());
      jobIds.add(jobStats);
    }
    return jobIds;
  }
}
