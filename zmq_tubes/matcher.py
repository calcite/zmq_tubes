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
        if key and key[-1] == '/':
            key = key[:-1]
        for sym in key.split('/'):
            node = node.children.setdefault(sym, self.TopicNode())
        node.content = value

    def get_topic(self, key, set_default=None):
        node = self._root
        if key and key[-1] == '/':
            key = key[:-1]
        for sym in key.split('/'):
            node = node.children.get(sym)
            if node is None:
                if set_default is not None:
                    self.set_topic(key, set_default)
                return set_default
        return node.content

    def filter(self, filter_topic: str):
        """
        Return registered topics by filter_topic
        :param filter_topic: str
        :return: [str]
        """
        def __rec(lst, node, _all=False, tt=None):
            if not lst and not node.children:
                return [('/'.join(tt), node.content)] if node.content else []
            part = None
            if _all:
                res = []
                for k, ch in node.children.items():
                    res += __rec([], ch, _all, tt + [k])
                return res
            elif lst and lst[0] in ['+', '#']:
                part = lst[0]
                lst = lst[1:]
                res = []
                for k, ch in node.children.items():
                    res += __rec(lst, ch, _all or part == '#', tt + [k])
                return res
            elif lst and lst[0] in node.children:
                return __rec(lst[1:], node.children[lst[0]], _all,
                             tt + [lst[0]])
            return []

        if filter_topic and filter_topic[-1] == '/':
            filter_topic = filter_topic[:-1]
        return __rec(filter_topic.split('/'), self._root, False, [])

    def matches(self, topic):
        if topic and topic[-1] == '/':
            topic = topic[:-1]
        lst = topic.split('/')
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
