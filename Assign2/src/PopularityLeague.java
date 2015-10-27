import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// >>> Don't Change
public class PopularityLeague extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(this.getConf(), "Title Count");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(PopularityLeagueMap.class);
		job.setReducerClass(PopularityLeagueReduce.class);
		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(PopularityLeague.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static List<String> readHDFSFile(String path, Configuration conf)
			throws IOException {
		Path pt = new Path(path);
		FileSystem fs = FileSystem.get(pt.toUri(), conf);
		FSDataInputStream file = fs.open(pt);
		BufferedReader buffIn = new BufferedReader(new InputStreamReader(file));

		List<String> everything = new ArrayList<String>();
		String line;
		while ((line = buffIn.readLine()) != null) {
			everything.add(line.trim().toLowerCase());
		}
		return everything;
	}

	// <<< Don't Change

	public static class PopularityLeagueMap extends
			Mapper<Object, Text, Text, IntWritable> {
		List<String> league;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();

			String leaguePath = conf.get("league");

			this.league = readHDFSFile(leaguePath, conf);
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] pages = line.trim().split(":");
			String parentPage = pages[0].trim().toLowerCase();

			if (this.league.contains(parentPage)) {
				context.write(new Text(parentPage), new IntWritable(0));
			}
			StringTokenizer tokenizer = new StringTokenizer(pages[1].trim(),
					" ");
			while (tokenizer.hasMoreTokens()) {
				if (this.league.contains(parentPage)) {
					context.write(new Text(tokenizer.nextToken().trim()
							.toLowerCase()), new IntWritable(1));
				}
			}
		}
	}

	public static class PopularityLeagueReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private List<Pair<String, Integer>> countToTitleList = new ArrayList<Pair<String, Integer>>();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int popularity = 0;
			for (IntWritable val : values) {
				popularity += val.get();
			}
			if (popularity != 0) {
				countToTitleList.add(new Pair<String, Integer>(key.toString(),
						popularity));
			}
		}

		@Override
		protected void cleanup(
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			Collections.sort(countToTitleList,
					new Comparator<Pair<String, Integer>>() {
						public int compare(Pair<String, Integer> o1,
								Pair<String, Integer> o2) {
							int valueCompare = o1.second.compareTo(o2.second);
							return valueCompare;
						}
					});
			int rank = countToTitleList.size();
			for (int i = 0; i < countToTitleList.size(); i++) {
				context.write(new Text(countToTitleList.get(i).first),
						new IntWritable(i));
			}
		}
	}
}

class Pair<A extends Comparable<? super A>, B extends Comparable<? super B>>
		implements Comparable<Pair<A, B>> {

	public final A first;
	public final B second;

	public Pair(A first, B second) {
		this.first = first;
		this.second = second;
	}

	public static <A extends Comparable<? super A>, B extends Comparable<? super B>> Pair<A, B> of(
			A first, B second) {
		return new Pair<A, B>(first, second);
	}

	@Override
	public int compareTo(Pair<A, B> o) {
		int cmp = o == null ? 1 : (this.first).compareTo(o.first);
		return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
	}

	@Override
	public int hashCode() {
		return 31 * hashcode(first) + hashcode(second);
	}

	private static int hashcode(Object o) {
		return o == null ? 0 : o.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Pair))
			return false;
		if (this == obj)
			return true;
		return equal(first, ((Pair<?, ?>) obj).first)
				&& equal(second, ((Pair<?, ?>) obj).second);
	}

	private boolean equal(Object o1, Object o2) {
		return o1 == o2 || (o1 != null && o1.equals(o2));
	}

	@Override
	public String toString() {
		return "(" + first + ", " + second + ')';
	}
}