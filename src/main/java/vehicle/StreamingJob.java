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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import vehicle.define.Event;
import vehicle.define.AccidentEvent;
import java.util.Iterator;
import org.apache.flink.api.common.functions.MapFunction;

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
public class StreamingJob {

	// Define Parameters.
    public static final String ACCIDENTS_FILE = "accidents.csv";
    static final String inputFile = "./traffic-3xways";
    static final String testFile = "./trafficTest"; 
    static final String outputFolder = "outputs";	
	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
        if (args.length < 2) {
            System.out.println("Usage: <input file> <output folder>");
            System.out.println("Usage: flink-quickstart-java-1.3.2.jar $PATH_TO_INPUT_FILE $PATH_TO_OUTPUT_FOLDER");
            throw new Exception();
        }
        String inputFile = args[0];
        String outputFolder = args[1];
        
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// read file
		// It is very important to set Parallelism to 1, otherwise there is no order guarantees across events because of flink's Parallel Dataflows
		DataStream<String> linesAsStrea = env.readTextFile(inputFile).setParallelism(1);
		
		
		DataStream<Event> eventAsStream= linesAsStrea.map(new MapFunction<String, Event>() {
				    /**
					 * user-defined mapFunction to convert Tuple8 to our defined class: Event
					 */
					private static final long serialVersionUID = 1L;

					Event event = new Event();
			    	
					public Event map(String csvLine) throws Exception {
						String[] tokens = csvLine.split(",");

				    	event.setTime(Integer.parseInt(tokens[0]));
				    	event.setVID(Integer.parseInt(tokens[1]));
				    	event.setSpd(Integer.parseInt(tokens[2]));
				    	event.setXWay(Integer.parseInt(tokens[3]));
				    	event.setLane(Integer.parseInt(tokens[4]));
				    	event.setDir(Integer.parseInt(tokens[5]));
				    	event.setSeg(Integer.parseInt(tokens[6]));
				    	event.setPos(Integer.parseInt(tokens[7]));
				        return event;
				    }					
				}).setParallelism(1);
		
		
		DataStream<AccidentEvent> outputAccidentEvent = eventAsStream
				.keyBy(new KeySelector<Event, Tuple2<Integer, Integer>>() {
                    /**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
                    public Tuple2<Integer, Integer> getKey(Event positionEvent) throws Exception {
                        return new Tuple2<>(
                                positionEvent.f1,
                                positionEvent.f7);
                    }
                })
                .countWindow(4, 1)
                .apply(new MyWindowFunction());

		outputAccidentEvent.writeAsCsv(outputFolder+"/"+ACCIDENTS_FILE, WriteMode.OVERWRITE).setParallelism(1);

		env.execute("Flink run As Stream");
	}
	
    static class MyWindowFunction implements WindowFunction<Event, AccidentEvent, Tuple2<Integer, Integer>, GlobalWindow> {

    	/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
			
		private AccidentEvent accidentEvent = new AccidentEvent();
	    private Event lastElement = new Event();
	
	    @Override
	    public void apply(Tuple2<Integer, Integer> key, GlobalWindow globalWindow,
	                      Iterable<Event> iterable, Collector<AccidentEvent> collector) throws Exception {
	
	        Iterator<Event> events = iterable.iterator();
	
	        int time1 = events.next().f0;
	        int count = 1;
	
	        while (events.hasNext()) {
	            count++;
	            lastElement = events.next();
	        }
	
	        if (count == 4) {
	            accidentEvent.setTime1(time1);
	            accidentEvent.setTime2(lastElement.f0);	     
	            accidentEvent.setVID(key.f0);
	            accidentEvent.setXWay(key.f1);
	            accidentEvent.setSeg(lastElement.f6);
	            accidentEvent.setDir(lastElement.f5);
	            accidentEvent.setPos(lastElement.f7);
	            collector.collect(accidentEvent);
	        }
	    }
    }
}


