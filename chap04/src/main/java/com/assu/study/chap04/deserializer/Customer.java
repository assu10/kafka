package com.assu.study.chap04.deserializer;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class Customer {
  private final int customerId;
  private final String customerName;
}
