package com.example.piglet.rest;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import com.example.piglet.jobs.MapReduceResult;
import com.example.piglet.jobs.MapReduceResult.JobStats;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.text.ParseException;
import java.util.*;

@Path("/piglet")
public class PigletResource {

  @GET
  @Path("/estimateCapacity")
  @Produces("application/json")
  public String estimate(@QueryParam("scriptName") String scriptPath, @QueryParam("sla") String sla) {
    StringBuilder mb = new StringBuilder();
	  MapReduceResult mrresult;
    System.out.println(" script Path : " + scriptPath + ", sla : " + sla);
    try {
       mrresult = PigletService.estimateCapacity("/tmp/piglet/scripts/"+scriptPath, sla);
       if(null == mrresult){
    	   mb.append("{");
    	   mb.append("\"success\":");
    	   mb.append(false);
    	   mb.append("}");
       }else{
    	   mb.append("{");
    	   mb.append("\"success\":");
    	   mb.append(true);
    	   
    	   mb.append(",\"oms\":");
    	   mb.append(mrresult.getNumMapSlots());
    	   
    	   mb.append(",\"ors\":");
    	   mb.append(mrresult.getNumReduceSlots());
    	   
    	   mb.append(",\"et\":");
    	   mb.append(mrresult.getExecTime());
    	   
    	   mb.append(",\"count\":");
         mb.append(mrresult.getIndividualJobsStats().size()) ;
    	   
    	   int counter = 1;
    	   for (JobStats stats : mrresult.getIndividualJobsStats()) {
    	     mb.append(",\"" + counter + "\":");
    	     mb.append("{");
    	     mb.append("\"jid\":");
    	     mb.append("\"" + stats.getJobId() + "\"");
    	     
    	     mb.append(",\"ms\":");
           mb.append(stats.getNumMapSlots());
           
           mb.append(",\"rs\":");
           mb.append(stats.getNumReduceSlots());
           
           mb.append(",\"st\":");
           mb.append(stats.getStartTimeSecs());
           
           mb.append(",\"ft\":");
           mb.append(stats.getFinishTimeSecs());
           mb.append("}");
           counter++;
    	   }
    	   mb.append("}");
       }
    } catch (Exception ex) {
    	ex.printStackTrace();
    	mb.append("{");
 	   mb.append("\"success\":");
 	   mb.append(false);
 	   mb.append("}");
    }
    return mb.toString();
  }

  @GET
  @Path("/upload")
  @Produces("application/json")
  public Response upload(@QueryParam("scriptName") String scriptName) {
    ResponseBuilder rb = Response.ok("OK");
    rb.header("Access-Control-Allow-Origin", "*");
    rb.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    
    try {
    	System.out.println("creating file");
    	(new File("/tmp/piglet/scripts/"+scriptName)).delete();
    	File file = new File("/tmp/piglet/scripts/"+scriptName);
    	file.createNewFile();
    } catch (Exception e) {
      //message = e.getMessage();
    }
    return rb.build();
  }
  
  @GET
  @Path("/writefile")
  @Produces("text/plain")
  public String writefile(@QueryParam("scriptName") String scriptName, @QueryParam("scriptBody") String scriptBody) {
    String message = "OK";
    try {
      uploadToHDFS(scriptName,scriptBody);
    } catch (Exception e) {
      message = e.getMessage();
    }
    return message;
  }

  private void uploadToHDFS(String scriptName, String scriptBody) {
	  try{
		  // Create file 
		  System.out.println("uploadToHDFS function---" + scriptBody);
		  File file = new File("/tmp/piglet/scripts/"+scriptName);
		  if(file.exists()){
			  FileOutputStream ff = new FileOutputStream(file, true);
			  ff.write(scriptBody.getBytes());
			  ff.close();
		  }
		  //FileWriter fstream = new FileWriter("/tmp/piglet/scripts/"+scriptName);
		  //BufferedWriter out = new BufferedWriter(fstream);
		  //out.write(scriptBody);
		  //Close the output stream
		  //out.close();
		  }catch (Exception e){//Catch exception if any
		  
		  }
    
  }
}
