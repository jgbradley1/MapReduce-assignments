import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfWritables;
import edu.umd.cloud9.util.TopNScoredObjects;
import edu.umd.cloud9.util.pair.PairOfObjectFloat;

public class ExtractTopTen extends Configured implements Tool {
    
    private static final String INPUT = "input";
    private static final String TOP = "top";
    private static final String SOURCES = "sources";
    
    private ExtractTopTen() {}
    
    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("top n").create(TOP));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("source nodes").create(SOURCES));

        CommandLine cmdline = null;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            System.exit(-1);
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(TOP) || !cmdline.hasOption(SOURCES)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(ExtractTopTen.class.getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(-1);
        }

        String indexPath = cmdline.getOptionValue(INPUT);
        String[] sources = cmdline.getOptionValues(SOURCES)[0].split(",");
        int top = Integer.parseInt(cmdline.getOptionValue(TOP));
        
        ArrayList<Integer> sourceList = new ArrayList<Integer>(sources.length);
        for (int i=0; i<sources.length; i++) {
            sourceList.add(i, Integer.parseInt(sources[i]));
        }
        
        // Create queue for each source node
        ArrayList<TopNScoredObjects<Integer>> queues = new ArrayList<TopNScoredObjects<Integer>>(sourceList.size());
        for (int i=0; i<sourceList.size(); i++) {
            queues.add(new TopNScoredObjects<Integer>(top));
        }
        
        
        
        List<PairOfWritables<IntWritable, PersonalizedPageRankNode>> PageRankResults =
                SequenceFileUtils.readDirectory(new Path(indexPath + "/part-m-00000"));
        
        for (PairOfWritables<IntWritable, PersonalizedPageRankNode> prResult : PageRankResults) {
            // the IntWritable is the nodeID, however this information is also available in the PersonalizedPageRankNode so we don't use the IntWritable
            
            //System.out.println(prResult.getLeftElement() + "\t" + prResult.getRightElement());
             
            PersonalizedPageRankNode node = prResult.getRightElement();
            // loop through for all source nodes
            for (int i=0; i<sourceList.size(); i++) {
                queues.get(i).add(node.getNodeId(), node.getPageRank(i));
            }
        }
        
        StringBuilder stringBuilder  = new StringBuilder();
        PairOfInts key = new PairOfInts();
        FloatWritable value = new FloatWritable();
        
        for (int i=0; i<sourceList.size(); i++) {
            String source = "Source:";
            int nodeid = sourceList.get(i);
            String result1 = String.format("%s %d", source, nodeid);
            stringBuilder.append(result1 + "\n");

            for (PairOfObjectFloat<Integer> pair : queues.get(i).extractAll()) {
                String keyString = pair.getLeftElement().toString();
                
                key.set(sourceList.get(i), Integer.parseInt(keyString));
                value.set(pair.getRightElement());
                
                float pagerank = (float) StrictMath.exp((double)pair.getRightElement());
                int nodeID = key.getRightElement();
                String result2 = String.format("%.5f %d", pagerank, nodeID);
                
                stringBuilder.append(result2 + "\n");
            }
            stringBuilder.append("\n");
        }
        
        // print out result
        System.out.println(stringBuilder.toString());
        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ExtractTopTen(), args);
    }
}
