package vehicle.define;

// Define POJOs of Event, SpeedingEvent, .
import org.apache.flink.api.java.tuple.Tuple8;

public class Event extends Tuple8<Integer, Integer, Integer,
Integer, Integer, Integer, Integer, Integer>{
	
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

    public Event() { }
    
	public int getTime() {
		return f0;
	}
	public int getVID() {
		return f1;
	}
	public int getSpd() {
		return f2;
	}
	public int getXWay() {
		return f3;
	}
	public int getLane() {
		return f4;
	}
	public int getDir() {
		return f5;
	}
	public int getSeg() {
		return f6;
	}
	public int getPos() {
		return f7;
	}	
	
	public void setTime(int Time) {
		f0 = Time;
	}
	public void setVID(int VID) {
		f1 = VID;
	}
	public void setSpd(int Spd) {
		f2 = Spd;
	}
	public void setXWay(int XWay) {
		f3 = XWay;
	}
	public void setLane(int Lane) {
		f4 = Lane;
	}
	public void setDir(int Dir) {
		f5 = Dir;
	}
	public void setSeg(int Seg) {
		f6 = Seg;
	}
	public void setPos(int Pos) {
		f7 = Pos;
	}
}



