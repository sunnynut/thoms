
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser; 
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;


public class city_median {

	public static class Map extends Mapper<Object,Text,Text,Text>{
		private static Text line=new Text();
		public void map(Object key,Text value,Context context)
			throws IOException,InterruptedException{ 
			String line = value.toString().replaceAll("\\001", ",");
			String city_id = line.split(",", 4)[2];
			String data  = line.split(",", 4)[3];
			if(data != null){
				context.write(new Text(city_id),new Text(data));
			}
		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text>{
                public Double median(ArrayList<Double> array) {
                  Collections.sort(array);
                  Double j = array.get(array.size()/2);
                  if(array.size()%2==0){
                    j=(array.get((array.size() - 1)/2)+array.get((array.size() - 1)/2+1))/2;
                  } else {
                    j=array.get((array.size() - 1)/2) ;
                  }
                  return j;
                }

                public void reduce(Text key,Iterable<Text> values,Context context)
                        throws IOException,InterruptedException{
                          ArrayList<Double> arr0 = new ArrayList<Double>();
                          ArrayList<Double> arr1 = new ArrayList<Double>();
                          ArrayList<Double> arr2 = new ArrayList<Double>();
                          ArrayList<Double> arr3 = new ArrayList<Double>();
                          ArrayList<Double> arr4 = new ArrayList<Double>();
                          ArrayList<Double> arr5 = new ArrayList<Double>();
                          ArrayList<Double> arr6 = new ArrayList<Double>();
                          ArrayList<Double> arr7 = new ArrayList<Double>();
                          ArrayList<Double> arr8 = new ArrayList<Double>();
                          ArrayList<Double> arr9 = new ArrayList<Double>();
                        for (Text value : values) {
                          arr0.add(Double.valueOf(value.toString().split(",")[0]).doubleValue());
                          arr1.add(Double.valueOf(value.toString().split(",")[2]).doubleValue());
                          arr2.add(Double.valueOf(value.toString().split(",")[4]).doubleValue());
                          arr3.add(Double.valueOf(value.toString().split(",")[6]).doubleValue());
                          arr4.add(Double.valueOf(value.toString().split(",")[8]).doubleValue());
                          arr5.add(Double.valueOf(value.toString().split(",")[10]).doubleValue());
                          arr6.add(Double.valueOf(value.toString().split(",")[12]).doubleValue());
                          arr7.add(Double.valueOf(value.toString().split(",")[14]).doubleValue());
                          arr8.add(Double.valueOf(value.toString().split(",")[16]).doubleValue());
                          arr9.add(Double.valueOf(value.toString().split(",")[18]).doubleValue());
                          //context.write(key, value);
                        }

                        Double order_cancel_rate = median(arr0);
                        Double order_reject_rate = median(arr1);
                        Double five_min_ready_rate = median(arr2);
                        Double ready_speed = median(arr3);
                        Double image_feedback_rate = median(arr4);
                        Double five_star_comment_rate = median(arr5);
                        Double comment_score = median(arr6);
                        Double comment_rate = median(arr7);
                        Double complain_rate = median(arr8);
                        Double punish_score_rate = median(arr9);
                        
                        String result = Double.toString(order_cancel_rate) + "	" + Double.toString(order_reject_rate) + "	" 
                          + Double.toString(five_min_ready_rate) + "	" + Double.toString(ready_speed) + "	" 
                          + Double.toString(image_feedback_rate) + "	" + Double.toString(five_star_comment_rate) + "	"
                          + Double.toString(comment_score) + "	" + Double.toString(comment_rate) + "	" 
                          + Double.toString(complain_rate) + "	" + Double.toString(punish_score_rate);
                        context.write(key, new Text(result));
                }
	}
	public static void main(String[] args) throws Exception{
 	       Configuration conf = new Configuration(); 	
	       String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
   	       if (otherArgs.length != 2) {
              	 System.err.println("Usage: city_median <in> <out>");
              	 System.exit(2);
               }
   	       Job job = new Job(conf, "city median");
               job.setJarByClass(city_median.class);
               job.setMapperClass(Map.class);
 	       //job.setCombinerClass(Reduce.class);
 	       job.setReducerClass(Reduce.class);
   	       job.setOutputKeyClass(Text.class);
   	       job.setOutputValueClass(Text.class);
  	       FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
  	       FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
  	       System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
