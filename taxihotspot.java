import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.Math;
import java.util.Map;

public class taxihotspot {

	public final static int MIN_LAT = 4050;
	public final static int MAX_LAT = 4090;
	public final static int MIN_LNG = -7425;
	public final static int MAX_LNG = -7370;

	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
		String outputFile = args[1];
		SparkConf conf = new SparkConf().setAppName("taxiHotspot");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input = sc.textFile(inputFile);

		JavaPairRDD<Record, Double> aggregation = input.mapToPair(l -> new Tuple2<>(parseInp(l), 1.0))//Mapreduce to count the occurance of each cell
				.filter(x -> boundarycheck(x._1())).reduceByKey((x, y) -> x + y);

		final double n = 40 * 55 * 31.;// this maximum of n, using this number
										// results in scores similar to the
										// sample scores
		final double mean = aggregation.map(x -> x._2()).reduce((x, y) -> x + y) / n;

		final double s = Math
				.sqrt(((aggregation.map(x -> x._2() * x._2()).reduce((x, y) -> x + y)) / n) - (mean * mean));
		final Map<Record, Double> broadcastevents = aggregation.collectAsMap();
		System.out.println("N: " + n);
		System.out.println("mean: " + mean);
		System.out.println("s: " + s);
		System.out.println("count: " + aggregation.count());

		JavaPairRDD<Record, Double> zScore = aggregation
				.mapToPair(keyvalue -> new Tuple2<>(z_score(keyvalue._1(), broadcastevents, mean, s, n), keyvalue._1()))
				.sortByKey(false).mapToPair(x -> new Tuple2<>(x._2(), x._1())); //Calculate z_Score of each cell

		zScore = JavaPairRDD.fromJavaRDD(sc.parallelize(zScore.filter(k -> !Double.isNaN(k._2)).take(50))); //Filter out points with no neoghbors and take the top 50

		System.out.println("N: " + n);
		System.out.println("mean: " + mean);
		System.out.println("s: " + s);
		System.out.println("count: " + aggregation.count());

		zScore.coalesce(1, true).saveAsTextFile(outputFile);

		sc.close();
	}

	public static double z_score(Record k, Map<Record, Double> broadcaste, double mean, double s, double num) { // Function to calculate zScore

		double[] nv = getNeighbours(k, broadcaste);
		double numerator = nv[0] - mean * nv[1];
		double denominator = s * Math.sqrt((num * nv[1] * 1.0 - nv[1] * nv[1] * 1.0) / (num * 1.0 - 1.0));
		return numerator / denominator;

	}

	private static Record parseInp(String inputline) {

		String[] temp_keys = inputline.split(",");
		if (temp_keys[5].equals("0") || temp_keys[1].equals("tpep_pickup_datetime")) {
			return new Record(-1, -1, -1);
		}

		int lat = (int) Math.floor(Float.parseFloat(temp_keys[6]) * 100) - taxihotspot.MIN_LAT;
		int lng = (int) Math.floor(Float.parseFloat(temp_keys[5]) * 100) - taxihotspot.MIN_LNG;
		int time = Integer.parseInt(temp_keys[1].split(" ")[0].split("-")[2]);

		Record ret = new Record(lat, lng, time);
		return ret;

	}

	private static boolean boundarycheck(Record inputline) { // Helper function to filter the noise data points 

		int lat = inputline.getlat();
		int lng = inputline.getlng();
		int time = inputline.getday();

		if (lat <= 39 && lat >= 0 && lng < 54 && lng >= 0 && time <= 31) {
			return true;
		} else {
			return false;
		}

	}

	private static double[] getNeighbours(Record Recordv, Map<Record, Double> broadcaste) { // Helper function to generate possible neighbours of a cell

		double[] toreturn = new double[2];
		toreturn[0] = 0.;
		toreturn[1] = 0.;

		int lat = Recordv.getlat();
		int lng = Recordv.getlng();
		int time = Recordv.getday();

		for (int i = lat - 1; i <= lat + 1; i++) {
			for (int j = lng - 1; j <= lng + 1; j++) {
				for (int k = time - 1; k <= time + 1; k++) {

					toreturn[1]++;

					Double x = broadcaste.get(new Record(i, j, k));

					if (x != null) {
						toreturn[0] += x;
					}
				}
			}
		}
		return toreturn;
	}
}

class Record implements Serializable {

	private int lng;
	private int lat;
	private int day;

	public Record(int x, int y, int z) {
		this.lat = x;
		this.lng = y;
		this.day = z;
	}

	public int getlng() {
		return lng;
	}

	public void setlng(int x) {
		this.lng = x;
	}

	public int getlat() {
		return lat;
	}

	public void setlat(int y) {
		this.lat = y;
	}

	public int getday() {
		return day;
	}

	public void setday(int z) {
		this.day = z;
	}

	@Override
	public String toString() {
		return (taxihotspot.MIN_LAT + lat) / 100. + ", " + (taxihotspot.MIN_LNG + lng) / 100. + ", " + (day - 1);
	}

	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (!(o instanceof Record))
			return false;
		Record newRecord = (Record) o;
		return newRecord.lat == this.lat && newRecord.lng == this.lng && newRecord.day == this.day;
	}

	@Override
	public int hashCode() {
		String hashStr = this.lat + "" + this.lng + "" + this.day;
		try {
			return Integer.parseInt(hashStr);
		} catch (NumberFormatException x) {

			return 0;
		}
	}
}
