import pytest

from tube.matcher import TopicMatcher


class TestTopicMatcher:

    @pytest.mark.parametrize("sub,topic", [
        ("foo/bar", "foo/bar"),
        ("foo/+", "foo/bar"),
        ("foo/+/baz", "foo/bar/baz"),
        ("foo/+/#", "foo/bar/baz"),
        ("A/B/+/#", "A/B/B/C"),
        ("#", "foo/bar/baz"),
        ("#", "/foo/bar"),
        ("/#", "/foo/bar"),
        ("$SYS/bar", "$SYS/bar"),
    ])
    def test_matching(self, sub, topic):
        t = TopicMatcher()
        t.set_topic(sub, True)
        assert t.match(topic)

    @pytest.mark.parametrize("sub,topic", [
        ("test/6/#", "test/3"),
        ("foo/bar", "foo"),
        ("foo/+", "foo/bar/baz"),
        ("foo/+/baz", "foo/bar/bar"),
        ("foo/+/#", "fo2/bar/baz"),
        ("/#", "foo/bar"),
        ("#", "$SYS/bar"),
        ("$BOB/bar", "$SYS/bar"),
    ])
    def test_not_matching(self, sub, topic):
        t = TopicMatcher()
        t.set_topic(sub, True)
        assert t.match(topic) is None
