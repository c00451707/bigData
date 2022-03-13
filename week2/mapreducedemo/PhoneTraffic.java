package week2;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneTraffic implements Writable {
    /**
     * 上行流量
     */
    private long up;

    /**
     * 下行流量
     */
    private long down;
    /**
     * 总流量
     */
    private long sum;

    public PhoneTraffic() {

    }

    public long getUp() {
        return up;
    }

    public long getDown() {
        return down;
    }

    public long getSum() {
        return sum;
    }




    public PhoneTraffic(long up, long down, long sum) {
        this.up = up;
        this.down = down;
        this.sum = sum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(up);
        dataOutput.writeLong(down);
        dataOutput.writeLong(sum);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.up = dataInput.readLong();
        this.down = dataInput.readLong();
        this.sum = dataInput.readLong();
    }

    @Override
    public String toString() {
        return up + "     " + down +  "     " + sum;
    }
}
