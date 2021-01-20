package objective;

import core.*;

import java.util.List;
import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.util.concurrent.*;
import java.util.HashMap;
import java.util.Date;
import java.text.*;
/**
 * Assignment:
 * Calculates the line of sight distance for each flight.
 * A multi-threaded solution which creates a mapper for the input file which maps each flight onto its nautical 
 * mile distance, each processed sequentially. Also error checks and corrects file input.
 *
 * To run:
 * java Task_4.java <files>
 *     i.e. java Task_4.java Top30_airports_LatLong.csv AComp_Passenger_data.csv
 * 
 * 
 * 
 * 
 * 
 *  
 */
class Task_4
    {
    // Configure and set-up the job using command line arguments specifying input files and job-specific mapper and
    // reducer functions
    private static AirportList aList=new AirportList(30);
    private static PassengerList pList=new PassengerList();
    public static void main(String[] args) throws Exception {
        ReadAndErrorCheck.run(args);
        aList=ReadAndErrorCheck.getAList();
        pList=ReadAndErrorCheck.getPList();
        Config config = new Config(mapper.class);
        Job job = new Job(config,pList);
        job.run();
        DisplayFlightMiles(Job.getMap());
    }
    
    // Function to calculate Nautical Mile distances based on https://www.geodata.source/developers/java accessed at 11.30am on 28th December 2020
    
    private static double CalcNauticalMiles(double latAIn, double lonAIn,double latBIn,double lonBIn ){
        double theta = lonAIn - lonBIn;
		double dist = Math.sin(Math.toRadians(latAIn)) * Math.sin(Math.toRadians(latBIn)) + Math.cos(Math.toRadians(latAIn)) * Math.cos(Math.toRadians(latBIn)) * Math.cos(Math.toRadians(theta));
		dist = Math.acos(dist);
		dist = Math.toDegrees(dist);
        dist = dist * 60 * 1.1515*0.8684;
        return dist;
    }
    private static void DisplayFlightMiles(ConcurrentHashMap<String,Object> mapIn){
        for (Map.Entry<String,Object> entry : mapIn.entrySet()){
            String key = entry.getKey();
            double value=Double.parseDouble(entry.getValue().toString());
            System.out.format("Flight: %-20s Nautical Miles: %.0f\n",key,value);
        }
    }
    // Flightid mapper:
    // Output nautical miles for each unique occurrence of Flightid.
    // KEY = Flightid
    // VALUE = Flight Nautical Miles
    public static class mapper extends Mapper {
        public void map(String line) {
            String[] Fields=line.split(",");
            double latA,lonA,latB,lonB;
            latA=aList.getLat(Fields[2]);
            latB=aList.getLat(Fields[3]);
            lonA=aList.getLon(Fields[2]);
            lonB=aList.getLon(Fields[3]);
            EmitIntermediate2(Fields[1],CalcNauticalMiles(latA, lonA, latB, lonB));
        }
    }
}