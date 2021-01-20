package core;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.*;
import java.util.concurrent.*;


/**
 * MapReduce Job Configuration
 * Stores the file specifications provided at run-time and
 * uses reflection to set objective-specific mapper and reducer classes.
 *
 * Areas for improvement:
 * - Output to file or implement a user interface to display results
 */
public class Config {
    // Input files to process
    private File[] files;

    // Classes to implement job-specific map function
    private Class mapper;

    // Constructor
    //public Config(String[] args, Class mapper, Class reducer, Class combiner) {
    public Config(String[] args, Class mapper){
        init(args);
        this.mapper = mapper;
    }

    // Initialise a job using the provided arguments
    private void init(String[] args) {
        if(args == null || args.length == 0) {
            System.out.println("Usage: java MapReduce <files>\n\tProcess a set of files listed by <files> using a trivial MapReduce implementation.");
            System.exit(1);
        }
        this.files = new File[args.length];
        for(int i=0; i<args.length; i++)
            this.files[i] = new File(args[i]);
    }

    
    // Generic file reader returning a list containing each line of input file
    protected static int read(File file) throws IOException {
    //List record = new ArrayList();
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    while((line = br.readLine()) != null)
        Job.record.add(line);
    br.close();
    System.out.println("Size of file ="+Job.record.size());
    return Job.record.size();
}
    // Return the list of files to process
    protected File[] getFiles() {
        return this.files;
    }

    // Using reflection get an instance of the mapper operating on a specified file
    protected Mapper getMapperInstance(File file) throws Exception {
        Mapper mapper = (Mapper) this.mapper.getConstructor().newInstance();
        mapper.setFile(file);
        return mapper;
    }
}