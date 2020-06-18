package io.listery;

import junit.framework.TestCase;

import java.text.ParseException;

public class UtilsTest extends TestCase {

  public void testTodayString() {
    String s = Utils.todayString();
    System.out.println(s);
  }

  public void testYesterdayString() throws ParseException {
    String s = Utils.yesterdayString("2020-10-01");
    System.out.println(s);
  }
}
