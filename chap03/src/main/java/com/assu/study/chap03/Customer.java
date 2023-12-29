package com.assu.study.chap03;

// 커스텀 시리얼라이저 - 고객 클래스
public class Customer {
  private int customerId;
  private String customerName;

  public Customer(int customerId, String customerName) {
    this.customerId = customerId;
    this.customerName = customerName;
  }

  public int getCustomerId() {
    return customerId;
  }

  public String getCustomerName() {
    return customerName;
  }
}
