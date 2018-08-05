package bean;

import java.io.Serializable;

public class Student2 implements Serializable {
    private String banji;
    private String banzhuren;

    public Student2(String banji, String banzhuren) {
        this.banji = banji;
        this.banzhuren = banzhuren;
    }

    public Student2() {
    }

    public String getBanji() {
        return banji;
    }

    public void setBanji(String banji) {
        this.banji = banji;
    }

    public String getBanzhuren() {
        return banzhuren;
    }

    public void setBanzhuren(String banzhuren) {
        this.banzhuren = banzhuren;
    }
}
