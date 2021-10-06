class TopicMatcher:

    class TopicNode(object):
        __slots__ = 'children', 'content'

        def __init__(self):
            self.children = {}
            self.content = None

    def __init__(self):
        self._root = self.TopicNode()

    def set_topic(self, key, value):
        node = self._root
        for sym in key.removesuffix('/').split('/'):
            node = node.children.setdefault(sym, self.TopicNode())
        node.content = value

    def get_topic(self, key, set_default=None):
        node = self._root
        for sym in key.removesuffix('/').split('/'):
            node = node.children.get(sym)
            if node is None:
                if set_default is not None:
                    self.set_topic(key, set_default)
                return set_default
        return node.content

    def matches(self, topic):
        lst = topic.removesuffix('/').split('/')
        lst_len = len(lst)
        normal = not topic.startswith('$')
        res = []

        def __rec(node, i=0):
            if i == lst_len:
                if node.content:
                    res.append(node.content)
            else:
                part = lst[i]
                if part in node.children:
                    __rec(node.children[part], i + 1)
                if '+' in node.children and (normal or i > 0):
                    __rec(node.children['+'], i + 1)
            if '#' in node.children and (normal or i > 0):
                content = node.children['#'].content
                if content:
                    res.append(content)
        __rec(self._root)
        return res

    def match(self, topic, default=None):
        res = self.matches(topic)
        if res:
            return res[0]
        return default

    def values(self) -> list:
        _values = []

        def __step(node):
            if node.content and node.content not in _values:
                _values.append(node.content)
            for child in node.children.values():
                __step(child)
        __step(self._root)
        return _values
