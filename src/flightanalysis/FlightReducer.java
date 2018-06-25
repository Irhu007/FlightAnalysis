/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package flightanalysis;

import java.util.*;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class FlightReducer extends Reducer<Text, FlightWritable, Text, Text> 
{
    ArrayList<String> busyFlights = new ArrayList<String>();
    private MultipleOutputs<Text,Text> out;
    
    @Override
     protected void setup(Context c)	throws IOException, java.lang.InterruptedException
    {
	out = new MultipleOutputs(c);
    }
     
    @Override
    public void reduce(Text key, Iterable<FlightWritable> value, Context con)throws IOException, InterruptedException
	{
            HashMap<String, String> traffic = new HashMap<>();
            int passenger =0;
            for(FlightWritable temp:value)
            {
                
                String[] date = temp.getDate().trim().split("-");
                String month = date[1];
                passenger = temp.getPassenger();
                int days = 1;
                if(traffic.containsKey(month))
                {
                    String[] str = traffic.get(month).trim().split(",");
                    days = Integer.parseInt(str[0])+1;
                    passenger = Integer.parseInt(str[1])+passenger;
                }
                traffic.put(month, ""+days+","+passenger);
            }
            int frequency = 0;
            int takeoffs = 0;
            
            for(Map.Entry<String,String> e: traffic.entrySet())
                {
                    String[] str = e.getValue().trim().split(",");
                    frequency = Integer.parseInt(str[1]);
                    takeoffs = Integer.parseInt(str[0]);
                    
                    if(takeoffs>25 && frequency<3000)
                        out.write("Low", key, new Text("Passengers: "+frequency+ " Month: "+e.getKey()));
                    
                    busyFlights.add(key+","+e.getKey()+","+frequency/takeoffs);
                }
        }
    @Override
    protected void cleanup(Context c) throws IOException, InterruptedException
    {
        Collections.sort(busyFlights, new Comparator<String>()
            {
                @Override
		public int compare(String s1, String s2)
		{
		    int fp1 = Integer.parseInt(s1.split(",")[2]);
		    int fp2 = Integer.parseInt(s2.split(",")[2]);
		    
		    return -(fp1-fp2);     /*For desscending order*/
		}});
        int count = 3;
        for(String x:busyFlights)
            {
                if(count!=0)
                {
                    String[] fields = x.split(",");
                    out.write("High",new Text(fields[0]),new Text(fields[1]+"\t"+fields[2]));
                    count--;
                }
            }
        //out.close();
    }
}
