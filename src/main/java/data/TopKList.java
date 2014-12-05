package data;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Created by Michael on 03.12.2014.
 */
public class TopKList<E extends Serializable> {
    CountMinSketch<E> sketch;
    PriorityQueue<E> heap;

    int k = Integer.MAX_VALUE;

    public TopKList(int k, CountMinSketch<E> sketch) {
        this.sketch = sketch;
        this.k = k;
        this.heap = new PriorityQueue<>(k, new
                SketchComparator<>(sketch));
    }

    public void add(E element, int c) {
        if (element != null) {
            sketch.increment(element, c);
            heap.add(element);

            while (heap.size() > k) {
                heap.remove();
            }
        }
    }

    public List<E> getTopKElements() {
        if (heap != null) {
            List<E> list = new LinkedList<>();

            Iterator<E> it = heap.iterator();
            while(it.hasNext()) {
                list.add(it.next());
            }

            return list;
        }

        return null;
    }

    public long getCount(E e) {
        return sketch.getCount(e);
    }
}
