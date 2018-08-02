package java8.functioninterfase;

/**
 * Created by MyPC on 2018/7/31.
 */
public interface FuncTest {
    default void testFun(Functional functional){
         functional.method(3223);
    }
}
