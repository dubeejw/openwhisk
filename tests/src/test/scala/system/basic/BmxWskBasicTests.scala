package system.basic

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

// TODO: Download bmx and login

@RunWith(classOf[JUnitRunner])
class BmxWskBasicTests extends WskBasicTests {
    // gradle tests:test -Dtest.single=BmxWskBasicTests -Dcli_path="/usr/local/bin/bx wsk"
}