package core;

import java.io.File;
import java.util.*;

//import objective.AirportList;
import java.util.concurrent.*;

/**
 * MapReduce Job class
 * Coordinates the entire MapReduce process
 *
 * Areas for improvement:
 * - Implement effective partitioning or "chunking" of the input to evenly distribute records across multiple mappers
 *   and reducers
 * - Pass segmented chunks to independent threads for efficient parallel processing
 * - Implement sorting
 */
public class Job {
    // Job configuration
    private Config config;

    // Global Threadsafe objects to store intermediate and final results
    protected static ConcurrentHashMap map,map1;
    protected static ArrayList record;

    // Constructor
    public Job(Config config) {
        this.config = config;
    }

    // Run the job given the provided configuration
    public void run() throws Exception {
        // Initialise the Threadsafe map to store intermediate results
        map = new ConcurrentHashMap();
        // Initialise ArrayList to read in file prior to chunking up
        record = new ArrayList<String>();
        // Execute the map and reduce phases in sequence
        map();
        System.out.println("After Map Phase, output map is: " + map);
    }

    // Map each provided file using an instance of the mapper specified by the job configuration
    private void map() throws Exception {
        for(File file : config.getFiles()) {
            Mapper mapper = config.getMapperInstance(file);
            mapper.run();
        }
    }
    public static ConcurrentHashMap getMap(){
        return map;      
    }
}