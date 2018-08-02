package java8.functioninterfase;

/**
 * Created by MyPC on 2018/7/31.
 */
@FunctionalInterface
public interface Functional {
    int method(int ee);
    default Integer sdc(Integer sdc){
        return sdc;
    }
}
