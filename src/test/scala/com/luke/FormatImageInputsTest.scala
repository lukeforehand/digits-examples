package com.luke

import java.io.File

import org.apache.commons.io.FileUtils
import org.junit.{Ignore, Test}

/**
 * Created by lukeforehand on 12/31/15.
 */
class FormatImageInputsTest {

  @Test
  @Ignore
  def test() {
    FileUtils.deleteQuietly(new File("target/output"))
    FormatImageInputs.main(Array(
      "src/main/resources/fall11_urls.txt",
      //"src/main/resources/10000_fall11_urls.txt",
      "src/main/resources/words.txt",
      "target/output/urls",
      "target/output/labels.txt",
      "1",
      "local[4]"
    ))
  }

}