package java8;

        import java8.functioninterfase.FuncTest;
        import java8.functioninterfase.Functional;
        import java8.functioninterfase.ImplClass;
        import org.junit.Test;

        import java.time.*;
        import java.time.format.DateTimeFormatter;
        import java.util.*;
        import java.util.function.Consumer;
        import java.util.function.Function;
        import java.util.stream.Collectors;
        import java.util.stream.Stream;

/**
 * Created by MyPC on 2018/7/31.
 */
public class FeatureJava8 {
    //Lambda表达式与Functional接口
    @Test
    public void demo1(){
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);
        list.sort((c,d) -> d.compareTo(c));
        list.removeIf(c->c>5);
        list.forEach(c-> System.out.println(c));
        Stream<String> asdc = Stream.of("sdcsadcsadcsdc");
        StringBuffer stringBuffer = new StringBuffer();
        asdc.map(c -> c.toUpperCase()).forEach(c->stringBuffer.append(c));
        System.out.println(stringBuffer.toString());
        Optional<Integer> reduce = Stream.of(list).flatMap(c -> c.stream()).reduce((a, b) -> a + b);
        System.out.println(reduce);
    }
    //日期测试类
    @Test
    public void demo2(){
        Clock clock = Clock.systemUTC();
        ZoneId zone = clock.getZone();
        String id = zone.getId();
        System.out.println(id);

        Instant instant = clock.instant();
        long millis = clock.millis();
        System.out.println(instant);
        System.out.println(millis);

        LocalDateTime localDateTime = LocalDateTime.now();
        String format = localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh-MM-ss"));
        System.out.println(format);
    }
}
