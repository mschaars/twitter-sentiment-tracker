package data;

import java.io.Serializable;

/**
 * Created by Michael on 03.12.2014.
 */
public class SketchItem implements Serializable {

    public Object key;
    public long value;

    public SketchItem(Object key, long value) {
        this.key = key;
        this.value = value;
    }
}
