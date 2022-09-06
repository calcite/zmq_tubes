import pytest

from zmq_tubes.matcher import TopicMatcher


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

    @pytest.mark.parametrize("filter,val", [
        ('#', [('g/p/+/i', 'i'), ('g/p/+/o', 'o'), ('g/s', 's')]),
        ('g/#', [('g/p/+/i', 'i'), ('g/p/+/o', 'o'), ('g/s', 's')]),
        ('g/s', [('g/s', 's')]),
        ('g/+/+/i', [('g/p/+/i', 'i')]),
        ('g/+/+/+', [('g/p/+/i', 'i'), ('g/p/+/o', 'o')]),
        ('g/+', [('g/s', 's')]),
    ])
    def test_filtering(self, filter, val):
        t = TopicMatcher()
        t.set_topic('g/p/+/i', 'i')
        t.set_topic('g/p/+/o', 'o')
        t.set_topic('g/s', 's')
        assert set(t.filter(filter)) == set(val)
