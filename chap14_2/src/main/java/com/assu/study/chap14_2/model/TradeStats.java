package com.assu.study.chap14_2.model;

public class TradeStats {
  String type;
  String ticker;
  int countTrades; //  평균 단가를 계산하기 위함
  double sumPrice;
  double minPrice;
  double avgPrice;

  public TradeStats add(Trade trade) {
    if (trade.type == null || trade.ticker == null) {
      throw new IllegalArgumentException("Invalid trade to aggregate: " + trade.toString());
    }

    if (this.type == null) {
      this.type = trade.type;
    }
    if (this.ticker == null) {
      this.ticker = trade.ticker;
    }

    if (!this.type.equals(trade.type) || !this.ticker.equals(trade.ticker)) {
      throw new IllegalArgumentException(
          "Aggregating stats for trade type: " + this.type + "and ticker: " + this.ticker);
    }

    if (countTrades == 0) {
      this.minPrice = trade.price;
    }

    this.countTrades = this.countTrades + 1;
    this.sumPrice = this.sumPrice + trade.price;
    this.minPrice = this.minPrice < trade.price ? this.minPrice : trade.price;

    return this;
  }

  // 평균 단가 계산
  public TradeStats computeAvgPrice() {
    this.avgPrice = this.sumPrice / this.countTrades;
    return this;
  }
}
