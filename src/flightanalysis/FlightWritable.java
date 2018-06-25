/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package flightanalysis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class FlightWritable implements Writable{

    private int passenger;
    private String date;    
    
    public FlightWritable()
    {
	set(0,"");
    }
    
    void set(int passenger, String date) {
        this.passenger = passenger;
        this.date = date;
    }
    
    int getPassenger()
    {
        return this.passenger;
    }
    
    String getDate()
    {
        return this.date;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeVInt(d, this.passenger);
        WritableUtils.writeString(d, this.date);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        this.passenger=WritableUtils.readVInt(di);
        this.date=WritableUtils.readString(di);
    }
    
}
