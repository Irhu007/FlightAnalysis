/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package flightanalysis;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper extends Mapper<LongWritable, Text, Text, FlightWritable>
{
    @Override
    public void map(LongWritable key, Text value, Context con)throws IOException, InterruptedException
    {
        String[] fields = value.toString().trim().split(",");
        FlightWritable details = new FlightWritable();
        details.set(Integer.parseInt(fields[4]),fields[5]);
        con.write(new Text(fields[0]), details);
        
    }
}
