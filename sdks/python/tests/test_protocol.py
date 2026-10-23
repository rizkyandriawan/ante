import io
import unittest

from ante.protocol import RespError, RespReader, encode


class ProtocolTests(unittest.TestCase):
    def test_encode_binary_safe(self):
        self.assertEqual(
            encode("PUBLISH", b"a\r\nb"),
            b"*2\r\n$7\r\nPUBLISH\r\n$4\r\na\r\nb\r\n",
        )

    def test_simple_integer_bulk(self):
        self.assertEqual(RespReader(io.BytesIO(b"+OK\r\n")).read(), "OK")
        self.assertEqual(RespReader(io.BytesIO(b":42\r\n")).read(), 42)
        self.assertEqual(RespReader(io.BytesIO(b"$3\r\nabc\r\n")).read(), b"abc")

    def test_null_bulk(self):
        self.assertIsNone(RespReader(io.BytesIO(b"$-1\r\n")).read())

    def test_nested_array(self):
        payload = b"*2\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
        self.assertEqual(RespReader(io.BytesIO(payload)).read(), [[b"a", b"b"], b"c"])

    def test_error(self):
        value = RespReader(io.BytesIO(b"-QUEUE_NOT_FOUND nope\r\n")).read()
        self.assertIsInstance(value, RespError)
        self.assertEqual(value.code, "QUEUE_NOT_FOUND")
        self.assertEqual(value.message, "nope")

    def test_truncated(self):
        with self.assertRaises(ConnectionError):
            RespReader(io.BytesIO(b"$5\r\nabc")).read()


if __name__ == "__main__":
    unittest.main()
