package reducetest;

import org.apache.hadoop.io.Text;

public class Airline {

	private int year;
	private int month;
	private int departureDelayTime;
	private int arriveDelayTime;
	private int distance;
	private boolean arriveDelayAvailable=true;
	private boolean departureDelayAvailable=true;
	private boolean distanceAvailable=true;
	private String uniqueCarrier;
	public Airline(Text text) {
		try {
			//csv 파일: ,로 데이터 구분
			String[] columns =text.toString().split(",");
			year=Integer.parseInt(columns[0]);	//년도
			month=Integer.parseInt(columns[1]);	//월
			uniqueCarrier=columns[8];	//항공사 코드
			if(!columns[15].equals("NA")) {	//NA: 출발지연의미 없다.
				departureDelayTime=Integer.parseInt(columns[15]); //출발했음
			}else {
				departureDelayAvailable=false;
			}
			if(!columns[14].equals("NA")) {
				arriveDelayTime = Integer.parseInt(columns[14]);
			}else {
				arriveDelayAvailable=false;
			}
			if(!columns[18].equals("NA")) {
				distance=Integer.parseInt(columns[18]);
			}else {
				distanceAvailable=false;
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	public int getYear() {
		return year;
	}
	public int getMonth() {
		return month;
	}
	public int getDepartureDelayTime() {
		return departureDelayTime;
	}
	public int getArriveDelayTime() {
		return arriveDelayTime;
	}
	public int getDistance() {
		return distance;
	}
	public boolean isArriveDelayAvailable() {
		return arriveDelayAvailable;
	}
	public boolean isDepartureDelayAvailable() {
		return departureDelayAvailable;
	}
	public boolean isDistanceAvailable() {
		return distanceAvailable;
	}
	public String getUniqueCarrier() {
		return uniqueCarrier;
	}
	

}