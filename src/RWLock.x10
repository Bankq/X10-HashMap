import x10.util.concurrent.Lock;

public class RWLock {
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