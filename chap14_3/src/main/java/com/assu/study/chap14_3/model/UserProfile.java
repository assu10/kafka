package com.assu.study.chap14_3.model;

public class UserProfile {
  int userId;
  String userName;
  String zipcode;
  String[] interests;

  public UserProfile(int userId, String userName, String zipcode, String[] interests) {
    this.userId = userId;
    this.userName = userName;
    this.zipcode = zipcode;
    this.interests = interests;
  }

  public UserProfile update(String zipcode, String[] interests) {
    this.zipcode = zipcode;
    this.interests = interests;
    return this;
  }

  public int getUserId() {
    return userId;
  }

  public String getUserName() {
    return userName;
  }

  public String getZipcode() {
    return zipcode;
  }

  public String[] getInterests() {
    return interests;
  }
}
