import x10.util.Timer;
import x10.util.ArrayList;
import x10.util.Pair;
import x10.util.concurrent.Lock;

/**
 * The Hash class includes two inner classes: Entry and RWlock.
 * A Rail with the type of Entry is the form how we store the data in the hashtable;
 * RWlock is implemented to manipulate the data safely.
 */
public class Hash
{
	/**
	 * each pair element <k, v> stored in the hashtable is an instance of Entry.
	 */
	static class Entry {
		/**
		 * each instance of Entry has a lock.
		 */
		static class RWLock {
			private var read_lock : Lock;
			private var writing : Boolean;
			private var read_count : long;
			
			public def this() {
				read_lock = new Lock();
				writing = false;
				read_count = 0;
			}
			
			public def readLock() {
				while (true) {
					read_lock.lock();
					if (read_count == 0) {
						if (writing) {
							read_lock.unlock();
							continue;
						} else {
							writing = true;
							break;
						}
					}
					read_lock.unlock();
				}
				read_count++;
				read_lock.unlock();
			}
			
			public def readUnlock() {
				read_lock.lock();
				if (read_count == 1) {
					writing = false;
				}
				read_count--;
				read_lock.unlock();
			}
			
			public def writeLock() {
				while (true) {
					read_lock.lock();
					if (writing) {
						read_lock.unlock();
						continue;
					} else {
						writing = true;
						break;
					}
				}
				read_lock.unlock();
			}
			
			public def writeUnlock() {
				writing = false;
			}
		}
		/** the key of the entry */
		public var k : long;
		
		/** the value of the entry */
		public var v : long;
		
		/** the lock of the entry */
		public var lock : RWLock;
		
		/** constructor that take a key and a value */
		public def this(a : long, b : long) {
			k = a;
			v = b;
			lock = new RWLock();
		}
	}
	/** the container of the hashtable */
    private var h : Rail[Entry];
    
    /** the capacity of the hashmap */
    private var capacity : long;
    
    /** the minimul actual size of the hashmap */
    private var size : double;
    
    /** the order of the action */
    private var count : long;
    
    /** the lock used to update the value of count */
    private var count_lock : Lock;
    
    /** the default value for the missing cases */
    private var defaultV : long;
    
    /** the load factor of the hashtable. default value is 0.8 which we can take advantage of the linear probing. */
    private var load_factor: double;
    
    /**
     * constructor of hash;
     */
    public def this(defV : long, workers : long , ratio : double , ins_per_thread : long , key_limit : long , value_limit : long){
    	load_factor=0.8;
    	count = 0;
        count_lock = new Lock();
        capacity = 1;
        /** calculate the minimul actual size that needed */
        size = (key_limit < ins_per_thread*workers?key_limit:ins_per_thread*workers)/load_factor;
        /** The actual table, must be of size 2**n */
        while ((capacity <<= 1) < size);
        defaultV = defV;
        h = new Rail[Entry](capacity);
        /** initailize the rail with <-1,-1>s */
        for (i in 0..(capacity - 1)) {
            h(i) = new Entry(-1, -1);
        }
    }
    
    /**
     * function that return the hash code for a key;
     * the hashcode should be in the range of [0, capacity).
     */
    private def hash(key : long) : long {
        return key.hashCode() & (this.capacity - 1);
    }
    
    /**
     * function that used to linear probing.
     */
    private def probe(cur_idx : long) : long {
        return (cur_idx + 1) & (capacity-1);
    }

    /**
     * Insert the pair <key,value> in the hash table
     *     'key'
     *     'value' 
     * This function return the unique order id of the operation in the linearized history.
     */
    public def put(key: long, value: long) : long
    {
    	/** get the hashcode. */
        var i : long = hash(key);
        var order : long;
        while (true) {
        	/** get the writelock for h(i) */
            h(i).lock.writeLock();
            /** 
             * h(i) is occupied by the initial entry, meaning this bucket is empty.
             * modify both the key and value.
             */
            if (h(i).k == -1) {
                count_lock.lock();
                order = ++count;
                h(i).k = key;
                h(i).v = value;
                count_lock.unlock();
                h(i).lock.writeUnlock();
                return order;
            }
            /** 
             * h(i) is occupied by the entry with the same key, meaning the key already existed.
             * just update the value, then.
             */
            else if (h(i).k == key) {
                count_lock.lock();
                order = ++count;
                h(i).v = value;
                count_lock.unlock();
                h(i).lock.writeUnlock();
                return order;
            }
            h(i).lock.writeUnlock();
            /**
             * h(i) is occupied by some entry with the same hashcode but different key. 
             * linear probe the rail.
             */
            i = probe(i);
        }
    }

    /**
     * get the value associated to the input key
     *     'key'
     */
    public def get(key: long) : Pair[long,long]
    {
    	/** get the hashcode */
        var i : long = hash(key);
        var value : long;
        var order : long;
        while (true) {
        	/** get the readlock for h(i) */
            h(i).lock.readLock();
            /**
             * h(i) is occupied by the initial entry, meaning the key doesn't exist;
             * return the default value and order.
             */
            if (h(i).k == -1) {
            	count_lock.lock();
            	order = ++count;
            	count_lock.unlock();
            	h(i).lock.readUnlock();
            	return new Pair[long, long](order, defaultV);
            } 
            /**
             * h(i) has the same key with the one we used to query.
             * return the order and the value.
             */
            else if (h(i).k == key) {
            	count_lock.lock();
            	order = ++count;
            	value = h(i).v;
            	count_lock.unlock();
            	h(i).lock.readUnlock();
            	return new Pair[long, long](order, value);
            }
            h(i).lock.readUnlock();
            /**
             * h(i) is occupied by some entry with the same hashcode but different key. 
             * linear probe the rail.
             */
            i = probe(i);
        }
    }
}