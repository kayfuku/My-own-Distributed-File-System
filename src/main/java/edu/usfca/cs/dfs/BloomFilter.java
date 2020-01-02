package edu.usfca.cs.dfs;

import java.util.ArrayList;
import java.util.BitSet;

import com.sangupta.murmur.Murmur3;

public class BloomFilter {

    private static final long SEED = 0x7f3a21a1;
    private int m_global; // The number of bits in the filter
    private int k_global; // The number of hash functions
    private int n_global; // The number of items in the filter
    private BitSet bloomFilter;

    /**
     * Constructor
     * Set the initial value of m_global and k_global
     */
    BloomFilter() {
        m_global = 100;
        k_global = 3;
        n_global = 0;
        bloomFilter = new BitSet(m_global);
    }

    /**
     * Constructor
     * @param m     The number of bits in the filter
     * @param k     The number of hash functions
     */
    BloomFilter(int m, int k) {
        m_global = m;
        k_global = k;
        n_global = 0;
        bloomFilter = new BitSet(m_global);
    }

    /**
     * First hash function
     * @param data  Data to be hashed
     * @return      The hash result
     */
    private long hash_1(byte[] data) {
        return Murmur3.hash_x86_32(data, data.length, SEED);
    }

    /**
     * Second hash function
     * @param data  Data to be hashed
     * @return      The hash result
     */
    private long hash_2(byte[] data) {
        return Murmur3.hash_x86_32(data, data.length, hash_1(data));
    }

    /**
     * The Kirsch-Mitzenmacher optimization for hash functions. 
     * @param data  Data to be hashed
     * @return      The hash result
     */
    private ArrayList<Long> hash_3(byte[] data) {
        ArrayList<Long> hashResult = new ArrayList<>();
        for (int i = 0; i < k_global; i++) {
            hashResult.add(hash_1(data) + (i * hash_2(data)));
        }
        return hashResult;
    }

    /**
     * Put function
     * Places the data in the filter
     * @param data
     */
    public void put(byte[] data) {
        if (data.length < 1) {
            return ;
        }
        ArrayList<Long> hashValues = hash_3(data);
        for (Long hashValue : hashValues) {
            bloomFilter.set((int) (hashValue % m_global));
//            System.out.println("put mod: " + (int) (hashValue % m_global));
		}
        n_global++;
        
//        System.out.println("bloomFilter: " + bloomFilter.toString());
    }

    /**
     * Get function
     * Reports the probability of whether or not
     * the data was put in the filter
     * @param data
     * @return      Return whether the data was put in the filter
     */
    public boolean get(byte[] data) {
        if (data.length < 1) {
            return false;
        }
        
        ArrayList<Long> hashValues = hash_3(data);
        for (Long hashValue : hashValues) {
//            System.out.println("get mod: " + (int) (hashValue % m_global));
            if(!bloomFilter.get((int) (hashValue % m_global))) {
            	return false;
            }
		}
        
        return true;
    }

    /**
     * Calculate falsePositiveProb
     * Number of items in the filter(n) = ceil(m / (-k / log(1 - exp(log(p) / k))))
     * falsePositiveProb(p) = pow(1 - exp(-k / (m / n)), k)
     * The number of bits in the filter(m) = ceil((n * log(p)) / log(1 / pow(2, log(2))))
     * The number of hash functions(k) = round((m / n) * log(2))
     * @return      Returns the false positive probability for the
     * filter given its current number of elements
     */
    public float falsePositiveProb() {
        float falsePositiveProb = 0;
        double mn = (double) m_global / (double) n_global;
        double kmn = (double) -k_global / (double) mn;
        double exp = Math.exp(kmn);
        falsePositiveProb = (float) Math.pow((double) (1 - exp), k_global);
//        System.out.println("m:" + m_global + ", n:" + n_global+",
//                      k:" + k_global + ", " + falsePositiveProb);
        return falsePositiveProb;
    }

}
