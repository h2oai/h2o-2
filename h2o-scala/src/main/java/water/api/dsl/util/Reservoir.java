package water.api.dsl.util;

import water.AutoBuffer;
import water.Iced;

import java.util.PriorityQueue;

/**
 * Simple utility to perform reservoir sampling in order to make huge data small enough to
 * bring in to local memory and display/etc.
 * I did this in Java instead of Scala, because I wasn't sure about Serialization/transient issues in Scala.
 *
 * Not Entirely Sure about threading/synchronization in this model
 */
public class Reservoir extends Iced {

    private transient final PriorityQueue<SampleItem> minHeap        = new PriorityQueue<SampleItem>();
    public            final int                       reservoirSize;

    //Needed For Serialization?
    public Reservoir() {
        this.reservoirSize = -1;
    }

    public Reservoir(int reservoirSize) {
        this.reservoirSize = reservoirSize;
    }

    @Override
    public water.AutoBuffer write(AutoBuffer bb) {
        int[]    order = new int[minHeap.size()];
        double[] vals  = new double[minHeap.size()]; //TODO: Till we figure out nulls

        int x=0;
        for(SampleItem item : minHeap) {
            order[x] = item.getRandomOrder();
            vals[x] = item.getValue();//==null?0:item.getValue(); //TODO: till we figure out nulls
            x++;
        }
        bb.put4(reservoirSize);
        bb.putA4(order);
        bb.putA8d(vals);
        return( bb );
    }

    @SuppressWarnings("unchecked")
    @Override
    public water.api.dsl.util.Reservoir read(AutoBuffer bb) {
        int           rSize         = bb.get4();
        int[]         order         = bb.getA4();
        double[]      vals          = bb.getA8d();
        Reservoir reservoir     = new Reservoir(rSize);

        for( int x=0;x<order.length;x++) {
            reservoir.minHeap.add(new SampleItem(order[x], vals[x]));
        }

        return(reservoir);
    }

    public void add(double item) {
        add( new SampleItem(item) );
    }

    synchronized public void add(SampleItem item) {
        if( item != null ) {
            if( minHeap.size() < reservoirSize) {
                minHeap.add(item);
            }else {
                SampleItem head = minHeap.peek();
                //If Item is > than the lest item in the heap.. then swap them out.
                if( item.getRandomOrder() > head.getRandomOrder() ) {
                    minHeap.poll();
                    minHeap.add(item);
                }
            }
        }
    }

//    synchronized public void merge( Reservoir other ) {
//        if( other != null ) {
//            for( SampleItem item : other.minHeap) {
//                add(item);
//            }
//        }
//    }
    synchronized public Reservoir merge( Reservoir other ) {
        Reservoir result = new Reservoir(this.reservoirSize);
        for(SampleItem item : this.minHeap ) {
            result.add(item);
        }
        if( other != null ) {
            for(SampleItem item : other.minHeap ) {
                result.add(item);
            }
        }
        return( result );
    }


    synchronized public double[] getValues() {
        double[] result = new double[minHeap.size()];

        int x=0;
        for(SampleItem item : minHeap) {
            result[x] = item.getValue();
            x++;
        }
        return( result );
    }

    synchronized public int getNumValues(){
        return( minHeap.size() );
    }

//    public static void main(String[] args) {
//        Reservoir reservoir = new Reservoir(10);
//        for( int x=0;x<15;x++) {
//            reservoir.add((double)x);
//        }
//    }
}
