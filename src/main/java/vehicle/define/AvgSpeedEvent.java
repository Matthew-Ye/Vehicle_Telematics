package vehicle.define;

import org.apache.flink.api.java.tuple.Tuple6;

public class AvgSpeedEvent extends Tuple6<Integer, Integer, Integer, Integer, Integer, Double>{
	
    /**
	 * AvgSpeedEvent for output
	 * Only for ouput, so we don't getFunctions.
	 * format:Time1,Time2,VID,XWay,Dir,AvgSpd
	 */
	private static final long serialVersionUID = 1L;

    public AvgSpeedEvent() { }
    
	
	public void setTime1(int Time1) {
		f0 = Time1;
	}
	public void setTime2(int Time2) {
		f1 = Time2;
	}
	public void setVID(int VID) {
		f2 = VID;
	}
	public void setXWay(int XWay) {
		f3 = XWay;
	}
	public void setDir(int Dir) {
		f4 = Dir;
	}
	public void setAvgSpd(Double AvgSpd) {
		f5 = AvgSpd;
	}
}
