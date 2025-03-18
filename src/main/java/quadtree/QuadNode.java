package quadtree;




import java.io.Serializable;


public class QuadNode<T> implements Serializable {
    QuadRectangle r;
    T element;

    QuadNode(QuadRectangle r, T element) {
        this.r = r;
        this.element = element;
    }

    public String toString() {
        return this.r.toString();
    }
    }
