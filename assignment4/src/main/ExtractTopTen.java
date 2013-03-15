import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import edu.umd.cloud9.io.pair.PairOfIntFloat;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.util.TopNScoredObjects;
import edu.umd.cloud9.util.pair.PairOfObjectFloat;

public class ExtractTopTen extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(ExtractTopTen.class);
    private static StringBuilder stringBuilder;

    private static class MyMapper extends
    Mapper<IntWritable, PersonalizedPageRankNode, PairOfInts, FloatWritable> {
        private ArrayList<TopNScoredObjects<Integer>> queues;

        // For holding the nodeID of all sources
        private static ArrayList<Integer> sources;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            stringBuilder = new StringBuilder();

            int k = conf.getInt("n", 100);

            String[] sourceList = conf.getStrings(SOURCES);
            sources = new ArrayList<Integer>(sourceList.length);
            for (int i=0; i<sourceList.length; i++) {
                sources.add(i, Integer.parseInt(sourceList[i]));
            }

            queues = new ArrayList<TopNScoredObjects<Integer>>(sources.size());
            for (int i=0; i<sources.size(); i++) {
                queues.add(new TopNScoredObjects<Integer>(k));
            }
        }

        @Override
        public void map(IntWritable nid, PersonalizedPageRankNode node, Context context) throws IOException,
        InterruptedException {
            
            System.out.println(node);
            // loop through for all source nodes
            for (int i=0; i<sources.size(); i++) {
                queues.get(i).add(node.getNodeId(), node.getPageRank(i));
            }
        }
        
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            PairOfInts key = new PairOfInts();
            FloatWritable value = new FloatWritable();
            
            for (int i=0; i<sources.size(); i++) {
                String source = "Source:";
                int nodeid = sources.get(i);
                String result1 = String.format("%s %d", source, nodeid);
                stringBuilder.append(result1 + "\n");

                for (PairOfObjectFloat<Integer> pair : queues.get(i).extractAll()) {
                    String keyString = pair.getLeftElement().toString();
                    
                    key.set(sources.get(i), Integer.parseInt(keyString));
                    value.set(pair.getRightElement());
                    
                    float pagerank = (float) StrictMath.exp((double)pair.getRightElement());
                    int nodeID = key.getRightElement();
                    String result2 = String.format("%.5f %d", pagerank, nodeID);
                    
                    stringBuilder.append(result2 + "\n");
                    context.write(key, value);
                }
                stringBuilder.append("\n");
            }
        }
    }
    
    
    public ExtractTopTen() {}
    
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String TOP = "top";
    private static final String SOURCES = "sources";
    private static Configuration conf;

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("top n").create(TOP));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("source nodes").create(SOURCES));
        
        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) ||
                !cmdline.hasOption(TOP) || !cmdline.hasOption(SOURCES)) {

            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int n = Integer.parseInt(cmdline.getOptionValue(TOP));
        String[] sources = cmdline.getOptionValues(SOURCES)[0].split(",");

        LOG.info("Tool name: " + ExtractTopTen.class.getSimpleName());
        LOG.info(" - input: " + inputPath);
        LOG.info(" - output: " + outputPath);
        LOG.info(" - top: " + n);

        Configuration conf = getConf();
        conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
        conf.setInt("n", n);
        conf.setStrings(SOURCES, sources);

        Job job = Job.getInstance(conf);
        job.setJobName(ExtractTopTen.class.getName() + ":" + inputPath);
        job.setJarByClass(ExtractTopTen.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(PairOfInts.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MyMapper.class);

        // Delete the output directory if it exists already.
        FileSystem.get(conf).delete(new Path(outputPath), true);

        job.waitForCompletion(true);

        return 0;
    }
    
    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ExtractTopTen(), args);

        LOG.info("\n\nRESULTS\n\n" + stringBuilder.toString());
        System.exit(res);
    }
}
