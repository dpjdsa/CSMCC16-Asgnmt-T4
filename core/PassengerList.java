package core;
import java.util.List;
import java.util.ArrayList;
/**
 * Class used to store list of passenger records
 * @author BD837672
 * @version 20th December 2020
 */
public class PassengerList {
    private List<Passenger> pList;
    /** Constructor
     * 
     */
    public PassengerList()
    {
        pList = new ArrayList<>();
    }
    // Add passenger to passenger record list
    public void addPassenger(Passenger passengerIn)
    {
        pList.add(passengerIn);
    }
    // Get size of passenger record list
    public int size()
    {
        return pList.size();
    }
    // Get passenger at position positionIn
    public Passenger getPassenger(int positionIn){
        return pList.get(positionIn);
    }
    @Override
    public String toString()
    {
        return pList.toString();
    }
}
