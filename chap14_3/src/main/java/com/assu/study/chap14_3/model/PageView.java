package com.assu.study.chap14_3.model;

public class PageView {
  int userId;
  String page;

  public PageView(int userId, String page) {
    this.userId = userId;
    this.page = page;
  }

  public int getUserId() {
    return userId;
  }

  public String getPage() {
    return page;
  }
}
