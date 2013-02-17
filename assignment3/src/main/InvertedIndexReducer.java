import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import edu.umd.cloud9.io.pair.PairOfInts;

//Reducer: sums up all the counts.
class InvertedIndexReducer extends Reducer<Text, PairOfInts, Text, Text> {

    // Reuse objects.
    private final static Text PostingsList = new Text();

    @Override
    public void reduce(Text key, Iterable<PairOfInts> values, Context context)
            throws IOException, InterruptedException {

        if (!key.toString().isEmpty()) {
            // Sum up values.
            int df = 0; // document frequency
            String postingList = "";
            boolean firstPair = true;
            for (PairOfInts docWordCount : values) {
                df += docWordCount.getRightElement();
                if (!firstPair)
                    postingList += ", " + docWordCount.toString();
                else {
                    postingList += docWordCount.toString();
                    firstPair = false;
                }
            }
            PostingsList.set(": " + df + " : " + postingList);
            context.write(key, PostingsList);
        }
    }
}