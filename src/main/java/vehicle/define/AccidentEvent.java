package vehicle.define;

import org.apache.flink.api.java.tuple.Tuple7;

public class AccidentEvent extends Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>{
	
    /**
	 * AccidentEvent for output
	 * Only for ouput, so we don't getFunctions.
	 * format: Time1, Time2, VID, XWay, Seg, Dir, Pos
	 */
	private static final long serialVersionUID = 1L;

    public AccidentEvent() { }
    
	
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
	public void setSeg(int Seg) {
		f4 = Seg;
	}
	public void setDir(int Dir) {
		f5 = Dir;
	}
	public void setPos(int Pos) {
		f6 = Pos;
	}
}
