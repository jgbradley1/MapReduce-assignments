import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import basic.PageRankNode.Type;

import edu.umd.cloud9.io.array.ArrayListOfFloatsWritable;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.map.HMapIFW;

/**
 * Representation of a graph node for Personalized PageRank. 
 *
 * @author Joshua Bradley
 */
public class PersonalizedPageRankNode implements Writable {
    public static enum Type {
        Complete((byte) 0),  // PageRank mass and adjacency list.
        Mass((byte) 1),      // PageRank mass only.
        Structure((byte) 2); // Adjacency list only.

        public byte val;

        private Type(byte v) {
            this.val = v;
        }
    };

    private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

    private Type type;
    private int nodeid;
    private ArrayListOfIntsWritable adjacenyList;
    private ArrayListOfFloatsWritable listOfPageRankValues;    // list of pagerank values, each with respect to a source node

    public PersonalizedPageRankNode() {
        listOfPageRankValues = new ArrayListOfFloatsWritable();
    }

    public float getPageRank(int index) {
        return this.listOfPageRankValues.get(index);
    }

    public void setPageRank(int index, float value) {
        if (index < listOfPageRankValues.size())
            this.listOfPageRankValues.set(index,  value);
        else
            this.listOfPageRankValues.add(index, value);
    }

    public int getNodeId() {
        return this.nodeid;
    }

    public void setNodeId(int n) {
        this.nodeid = n;
    }

    public ArrayListOfIntsWritable getAdjacenyList() {
        return this.adjacenyList;
    }

    public void setAdjacencyList(ArrayListOfIntsWritable list) {
        this.adjacenyList = list;
    }

    public Type getType() {
        return this.type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    /**
     * Deserializes this object.
     *
     * @param in source for raw byte representation
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        int b = in.readByte();
        type = mapping[b];
        nodeid = in.readInt();
        
        if (type.equals(Type.Mass)) {
            this.listOfPageRankValues = new ArrayListOfFloatsWritable();
            this.listOfPageRankValues.readFields(in);
            return;
        }

        if (type.equals(Type.Complete)) {
            this.listOfPageRankValues = new ArrayListOfFloatsWritable();
            this.listOfPageRankValues.readFields(in);
        }
        
        adjacenyList = new ArrayListOfIntsWritable();
        adjacenyList.readFields(in);
    }

    /**
     * Serializes this object.
     *
     * @param out where to write the raw byte representation
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(type.val);
        out.writeInt(nodeid);
        
        if (type.equals(Type.Mass)) {
            this.listOfPageRankValues.write(out);
            return;
        }

        if (type.equals(Type.Complete)) {
            this.listOfPageRankValues.write(out);
        }
        
        adjacenyList.write(out);
    }

    @Override
    public String toString() {
        return String.format("{%d %s %s}",
                nodeid,
                (listOfPageRankValues == null ? "[]" : listOfPageRankValues.toString()),
                (adjacenyList == null ? "[]" : adjacenyList.toString(this.adjacenyList.size())));
    }


    /**
     * Returns the serialized representation of this object as a byte array.
     *
     * @return byte array representing the serialized representation of this object
     * @throws IOException
     */
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(bytesOut);
        write(dataOut);

        return bytesOut.toByteArray();
    }

    /**
     * Creates object from a <code>DataInput</code>.
     *
     * @param in source for reading the serialized representation
     * @return newly-created object
     * @throws IOException
     */
    public static PersonalizedPageRankNode create(DataInput in) throws IOException {
        PersonalizedPageRankNode m = new PersonalizedPageRankNode();
        m.readFields(in);

        return m;
    }

    /**
     * Creates object from a byte array.
     *
     * @param bytes raw serialized representation
     * @return newly-created object
     * @throws IOException
     */
    public static PersonalizedPageRankNode create(byte[] bytes) throws IOException {
        return create(new DataInputStream(new ByteArrayInputStream(bytes)));
    }
}
