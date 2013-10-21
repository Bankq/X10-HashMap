import x10.util.concurrent.Lock;

public class RWLock {
    private var read_lock : Lock;
    private var write_lock : Lock;
    private var read_count : long;
    
    public def this() {
        this.read_lock = new Lock();
        this.write_lock = new Lock();
        this.read_count = 0;
    }
    
    public def readLock() {
        this.read_lock.lock();
        if (++this.read_count == 1) {
            this.write_lock.lock();
        }
        this.read_lock.unlock();
    }
    
    public def readUnlock() {
        this.read_lock.lock();
        if (--this.read_count == 0) {
            this.write_lock.unlock();
        }
        this.read_lock.unlock();
    }
    
    public def writeLock() {
        this.write_lock.lock();
    }
    
    public def writeUnlock() {
        this.write_lock.unlock();
    }
}