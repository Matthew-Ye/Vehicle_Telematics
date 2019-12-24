package vehicle.define;

import org.apache.flink.api.java.tuple.Tuple6;

public class SpeedingEvent extends Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>{
	
    /**
	 * SpeedingEvent for output
	 * Only for ouput, so we don't getFunctions.
	 * format:Time,VID,XWay,Seg,Dir,Spd
	 */
	private static final long serialVersionUID = 1L;

    public SpeedingEvent() { }
    
	
	public void setTime(int Time) {
		f0 = Time;
	}
	public void setVID(int VID) {
		f1 = VID;
	}
	public void setXWay(int XWay) {
		f2 = XWay;
	}
	public void setSeg(int Seg) {
		f3 = Seg;
	}
	public void setDir(int Dir) {
		f4 = Dir;
	}
	public void setSpd(int Spd) {
		f5 = Spd;
	}
}
