package vehicle;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import vehicle.define.Event;
import vehicle.define.SpeedingEvent;
import vehicle.define.AvgSpeedEvent;
import org.apache.flink.api.common.functions.GroupReduceFunction;

/**
 * Skeleton for a Flink Batch Job.
 *
 * For a full example of a Flink Batch Job, see the WordCountJob.java file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flink-quickstart-java-1.3.2.jar
 * From the CLI you can then run
 * 		./bin/flink run -c vehicle.BatchJob target/flink-quickstart-java-1.3.2.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class BatchJob {

	// Define Parameters.
    static final String SPEED_RADAR_FILE = "speedfines.csv";
    static final String AVGSPEED_FILE = "avgspeedfines.csv";
    static final int MAXIMUM_SPEED = 90;
	static final double MAXAVGSPEED=60;//1 metre per second = 2.23694 mph
//    static final String inputFile = "./traffic-3xways";
//    static final String testFile = "./trafficTest";     
//    static final String outputFolder = "outputs";	
	public static void main(String[] args) throws Exception {
		
        if (args.length < 2) {
            System.out.println("Usage: <input file> <output folder>");
            System.out.println("Usage: flink-quickstart-java-1.3.2.jar $PATH_TO_INPUT_FILE $PATH_TO_OUTPUT_FOLDER");
            throw new Exception();
        }
        String inputFile = args[0];
        String outputFolder = args[1];
        
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		// read a CSV file 
		
		DataSet<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> lines = env.readCsvFile(inputFile)
			                       .types(Integer.class, Integer.class, Integer.class, Integer.class, Integer.class, Integer.class, Integer.class, Integer.class);

		
		DataSet<Event> events = lines.map(new MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Event>() {
		    /**
			 * user-defined mapFunction to convert Tuple8 to our defined class: Event
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Event map(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> csvLine) throws Exception {
		    	Event event = new Event();
		    	event.setTime(csvLine.f0);
		    	event.setVID(csvLine.f1);
		    	event.setSpd(csvLine.f2);
		    	event.setXWay(csvLine.f3);
		    	event.setLane(csvLine.f4);
		    	event.setDir(csvLine.f5);
		    	event.setSeg(csvLine.f6);
		    	event.setPos(csvLine.f7);	
		        return event;
		    }
		});		

		DataSet<Event> filteredSpdEvents = events.filter(new FilterFunction<Event>() {
		    /**
			 * user-defined filterFunction to choose the speeding vehicles
			 */
			private static final long serialVersionUID = 1L;
			@Override
		    public boolean filter(Event event) throws Exception {
		    	if(event.getSpd()>MAXIMUM_SPEED) {
		    		return true;
		    	}
		    	else{
		    		return false;
		    	}
		    }
		});
				
		DataSet<SpeedingEvent> outputSpeedingEvent = filteredSpdEvents.map(new MapFunction<Event, SpeedingEvent>() {
		    /**
			 * user-defined mapFunction to output the information of speeding vehicles in format.
			 */
			private static final long serialVersionUID = 1L;

	        SpeedingEvent thisSpeedingEvent = new SpeedingEvent();
	        public SpeedingEvent map(Event event) throws Exception {
	        	thisSpeedingEvent.setTime(event.getTime());
	        	thisSpeedingEvent.setVID(event.getVID());
	        	thisSpeedingEvent.setXWay(event.getXWay());
	        	thisSpeedingEvent.setSeg(event.getSeg());
	        	thisSpeedingEvent.setDir(event.getDir());
	        	thisSpeedingEvent.setSpd(event.getSpd());
	            return thisSpeedingEvent;
	        }
		});		

		DataSet<Event> filteredAvgSpdEvents = events.filter(new FilterFunction<Event>() {
		    /**
			 * user-defined filterFunction to choose the events whose segments are between 52 and 56
			 */
			private static final long serialVersionUID = 1L;
			@Override
		    public boolean filter(Event event) throws Exception {
		    	if(event.getSeg()>=52&&event.getSeg()<=56) {
		    		return true;
		    	}
		    	else{
		    		return false;
		    	}
		    }
		});
		
		
		DataSet<AvgSpeedEvent> outputAvgSpeedEvent = filteredAvgSpdEvents.groupBy(1)
		.reduceGroup(new GroupReduceFunction<Event,AvgSpeedEvent>(){
			/**
			 * caculate the avgSpeed and filter
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void reduce(Iterable<Event> in, Collector<AvgSpeedEvent> out) {
				Integer maxPos = 0;
				Integer minPos = Integer.MAX_VALUE;
				Integer maxTime = 0;
				Integer minTime =Integer.MAX_VALUE;
				Integer VID=null;
				Integer XWay=null;
				Integer Dir=null;
				Integer minSeg=null;
				Integer maxSeg=null;
				
				for (Event i:in) {
					VID=i.f1;
					XWay=i.f3;
					Dir=i.f5;
					if (i.f7>maxPos) {
						maxPos=i.f7;
						maxTime=i.f0;
						maxSeg=i.f6;

					}
					if (i.f7<minPos) {
						minPos=i.f7;
						minTime=i.f0;
						minSeg=i.f6;
					}
				}
				
				
			    /**
				 calucate the average speed
				 1 ms = 2.23694 mph
				 both directions are required
				 when the direction == 0, which means it was for Eastbound, the maxTime-minTime >= 0
				 when the direction == 1, which means it was for Westbound, the maxTime-minTime <=0
				 Therefore, we need MATH.abs to get the absolute value.
				 */	
				double avgSpeed=Math.abs((maxPos-minPos)*1.0/(maxTime-minTime)*2.236936292054402);
				// Cars that do not complete the segment (52-56) are not taken into account by the average speed control. For example 52->54 or 55->56.
				// So maxSeg==56 and minSeg==52 are required as complete detections.
				if(avgSpeed>MAXAVGSPEED&&minTime!=maxTime&&maxSeg==56&&minSeg==52) {
					AvgSpeedEvent output= new AvgSpeedEvent(); 
					output.setTime1(minTime);
					output.setTime2(maxTime);
					output.setVID(VID);
					output.setXWay(XWay);
					output.setDir(Dir);
					output.setAvgSpd(avgSpeed);
					out.collect(output);
				}
			}				
		});

		outputSpeedingEvent.writeAsCsv(outputFolder+"/"+SPEED_RADAR_FILE, WriteMode.OVERWRITE).setParallelism(1);
		outputAvgSpeedEvent.writeAsCsv(outputFolder+"/"+AVGSPEED_FILE, WriteMode.OVERWRITE).setParallelism(1);


		env.execute("Flink run As Batch");
	}
	
}


