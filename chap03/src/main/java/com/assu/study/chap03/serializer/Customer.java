package com.assu.study.chap03.serializer;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class Customer {
  private final int customerId;
  private final String customerName;
}
