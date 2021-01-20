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
import java.math.*;
/**
 * Assignment Objective 4:
 * Reads in the airport list, the passenger flight records and using the airport codes
 * calculates nautical mile distance for each flight.
 * A single threaded solution which creates a mapper for each file and a reducer for each unique word,
 * each processed sequentially.
 *
 * To run:
 * java Task_4.java <files>
 *     i.e. java  Task_4.java AComp_Passenger_data_no_error.csv
 *
 * Potential Areas for improvement:
 * 
 * - Multi-threading
 * - Error checking and handling
 * 
 *   
 */
class Task_4
    {
    // Configure and set-up the job using command line arguments specifying input files and job-specific mapper and
    // reducer functions
    private static AirportList aList=new AirportList(30);
    public static void main(String[] args) throws Exception {
        ReadAirports();
        Config config = new Config(args, mapper.class, reducer.class, combiner.class);
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
    public static void ReadPassengers()
    {
        String csvFile2="AComp_Passenger_data_no_error.csv";
        BufferedReader br = null;
        String line = "";
        PassengerList pList=new PassengerList();
        try {
                br = new BufferedReader(new FileReader(csvFile2));
                while((line=br.readLine())!=null){
                    if (line.length()>0){
                        String[] Field = line.split(",");
                        String passid=Field[0];
                        String fltid=Field[1];
                        String frmapt=Field[2];
                        String dstapt=Field[3];
                        double deptime=Double.parseDouble(Field[4]);
                        double flttime=Double.parseDouble(Field[5]);
                        Passenger passenger = new Passenger(passid,fltid,frmapt,dstapt,deptime,flttime);
                        pList.addPassenger(passenger);
                    }
                }
                br.close();
        } catch (IOException e) {
            System.out.println("IO Exception");
            e.printStackTrace();
        }
        System.out.println(pList);
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
            System.out.format("Flight: %-20s  Nautical Miles: %.0f\n",key,value);
        }
    }

    // FromAirportCode+Flightid count mapper:
    // Output nautical miles for each occurrence of FlightId+PassengerId.
    // KEY = FlightId+PassengerId
    // VALUE = Flight nautical miles
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
    // Airport Code count combiner (not used for this task):
    // Output the total number of occurrences of each unique Aiport Code
    // KEY = FromAirport Code
    // VALUE = count
    public static class combiner extends Combiner {
        public void combine(String key, List values) {
            //int count = 0;
            //for (Object value : values) count += (int) value;
            EmitIntermediate3(key.toString().substring(0,3), values);
        }
    }
    // Airport Code count reducer (not used for this task):
    // Output the total number of occurrences of each unique Aiport Code
    // KEY = FromAirport Code
    // VALUE = count
    public static class reducer extends Reducer {
        public void reduce(String key, List values) {
            int count = 0;
            for (Object lst : values){
                for (Object value : (List) lst) count += (int) value;
                Emit(key, count);
            }
        }
    }
}