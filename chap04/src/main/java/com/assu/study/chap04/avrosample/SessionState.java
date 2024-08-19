package com.assu.study.chap04.avrosample;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
@Getter
@Setter
public class SessionState {
  private final long lastConnection;
  private final int sessionId;
}
