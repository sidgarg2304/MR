import Mapper.Map;
import Mapper.Reduce;

public class Main {
	

public static void main(String[] args) throws Exception {


Configuration conf = new Configuration();



Job job = Job.getInstance(conf, "Alisha_Program");


job.setMapperClass(Map.class);

job.setReducerClass(Reduce.class);



job.setOutputKeyClass(Text.class);

job.setOutputValueClass(Text.class);


job.setInputFormatClass(TextInputFormat.class);

job.setOutputFormatClass(TextOutputFormat.class);



FileInputFormat.addInputPath(job, new Path(args[0]));

FileOutputFormat.setOutputPath(job, new Path(args[1]));



job.waitForCompletion(true);


}



}



}
