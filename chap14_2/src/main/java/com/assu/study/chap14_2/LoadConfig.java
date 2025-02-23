package com.assu.study.chap14_2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/** 파일에서 구성 로드 주로 연결 정보를 위한 것으로 다시 컴파일하지 않고도 클러스터 간에 전환할 수 있음 */
public class LoadConfig {
  private static final String DEFAULT_CONFIG_FILE =
      System.getProperty("user.home") + File.separator + ".ccloud" + File.separator + "config";

  static Properties loadConfig() throws IOException {
    return loadConfig(DEFAULT_CONFIG_FILE);
  }

  static Properties loadConfig(String configFile) throws IOException {
    if (!Files.exists(Paths.get(configFile))) {
      throw new RuntimeException(configFile + "does not exists.");
    }

    final Properties cfg = new Properties();

    try (InputStream inputStream = new FileInputStream(configFile)) {
      cfg.load(inputStream);
    }
    return cfg;
  }
}
