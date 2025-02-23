package com.assu.study.chap14_2.model;

public class Trade {
  String type;
  String ticker;
  double price;
  int size;

  public Trade(String type, String ticker, double price, int size) {
    this.type = type;
    this.ticker = ticker;
    this.price = price;
    this.size = size;
  }

  @Override
  public String toString() {
    return "Trade{"
        + "type='"
        + type
        + '\''
        + ", ticker='"
        + ticker
        + '\''
        + ", price="
        + price
        + ", size="
        + size
        + '}';
  }
}
