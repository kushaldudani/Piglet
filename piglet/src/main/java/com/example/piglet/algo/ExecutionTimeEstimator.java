package com.example.piglet.algo;

import java.util.Collection;
import java.util.Map;

import com.example.piglet.jobs.JobInfo;
import com.example.piglet.jobs.JobNode;

public interface ExecutionTimeEstimator {
   long estimateTotalExecTime(int numMapSlots, int numReduceSlots);
   
   
}
