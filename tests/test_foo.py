from spark_rock_jvm_python.foo import foo


def test_foo():
    assert foo("foo") == "foo"
