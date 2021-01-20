package objective;

import core.*;

import java.util.List;
import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.util.concurrent.*;
import java.util.HashMap;
/**
 * Assignment Task 4:
 * Calculates the line of sight distance for each flight.
 * A multi-threaded solution which creates a mapper for the input file.
 * each processed sequentially.
 *
 * To run:
 * java Task_4.java <files>
 *     i.e. java Task_4.java AComp_Passenger_data_no_error.csv
 *
 * Potential Areas for improvement:
 * 
 * - Error checking and handling
 * 
 *   
 */
class Task_4
    {
    // Configure and set-up the job using command line arguments specifying
    // input files and job-specific mapper function.
    private static AirportList aList=new AirportList(30);
    public static void main(String[] args) throws Exception {
        ReadAirports();
        Config config = new Config(args, mapper.class);
        Job job = new Job(config);
        job.run();
        DisplayFlightMiles(Job.getMap());
    }
    // Read in airports file
    public static void ReadAirports()
    {
        String csvFile1="Top30_airports_LatLong.csv";
        BufferedReader br = null;
        String line = "";
        try {
                br = new BufferedReader(new FileReader(csvFile1));
                while((line=br.readLine())!=null){
                    if (line.length()>0){
                        String[] Field = line.split(",");
                        String name=Field[0];
                        String code=Field[1];
                        double lat=Double.parseDouble(Field[2]);
                        double lon=Double.parseDouble(Field[3]);
                        Airport airport = new Airport(name,code,lat,lon);
                        aList.addAirport(airport);
                    }
                }
                br.close();
        } catch (IOException e) {
            System.out.println("IO Exception");
            e.printStackTrace();
        }
        System.out.println(aList);
        System.out.println("*** no of airports is: "+aList.size());
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