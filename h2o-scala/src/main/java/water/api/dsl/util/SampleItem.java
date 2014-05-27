package water.api.dsl.util;

import java.util.Random;

import water.Iced;

/**
 * Wraps an value, with a random int (Used in reservoir sampling)
 */
public class SampleItem extends Iced implements Comparable<SampleItem> {
    private static final Random rand = new Random();
    public final int    randomOrder;
    public final double value;

    public SampleItem(int randomOrder, double value) {
        this.randomOrder = randomOrder;
        this.value = value;
    }

    public SampleItem(double value) {
        this.value       = value;
        this.randomOrder = rand.nextInt();
    }

    public int getRandomOrder() {
        return randomOrder;
    }

    public double getValue() {
        return value;
    }

    @Override
    public int compareTo(SampleItem that) {
        if(this == that) {
          return 0;
        }else if(that == null ) {
          return 1;
        }else {
          return this.randomOrder - that.randomOrder ;
        }
    }

    @Override
    public String toString() {
        return "SampleItemStub{" +
                "randomOrder=" + randomOrder +
                ", value=" + value +
                '}';
    }
}
