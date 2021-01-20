package core;

import java.io.File;
import java.util.*;

//import objective.AirportList;
import java.util.concurrent.*;

/**
 * MapReduce Job class
 * Coordinates the entire MapReduce process
 *
 * Features:
 * - Implements effective partitioning or "chunking" of the input to evenly distribute records across multiple mappers
 *   and reducers
 * - Passes segmented chunks to independent threads for efficient parallel processing
 * 
 */
public class Job {
    // Job configuration
    private Config config;

    // Global Threadsafe objects to store intermediate and final results
    protected static ConcurrentHashMap map,map1;
    protected static ArrayList record;
    protected static PassengerList pList;

    // Constructor
    public Job(Config config,PassengerList pListIn) {
        this.config = config;
        this.pList=pListIn;
    }

    // Run the job given the provided configuration, only requires map phase
    public void run() throws Exception {
        // Initialise the Threadsafe maps to store intermediate results
        map = new ConcurrentHashMap();
        map1 = new ConcurrentHashMap();
        // Initialise ArrayList to read in file prior to chunking up
        record = new ArrayList<String>();
        // Execute the map and phase
        map();
        System.out.println("After Map Phase, output map is: " + map);
    }

    // Map each provided file using an instance of the mapper specified by the job configuration
    private void map() throws Exception {
            Mapper mapper = config.getMapperInstance();
            mapper.setPList(pList);
            mapper.run();
        
    }
    public static ConcurrentHashMap getMap(){
        return map;      
    }
}