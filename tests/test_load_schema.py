import yaml

from zmq_tubes import TubeNode


def test_load_schema_simple():

    schema = """
    tubes:
      - name: test_tube
        addr:  ipc:///tmp/test.pipe
        server: yes
        tube_type: REP
        identity: XXX
        topics:
          - foo/#
          - +/bar
    """

    node = TubeNode(schema=yaml.safe_load(schema))

    assert len(node.tubes) == 1
    assert node.tubes[0].name == 'test_tube'
    assert node.tubes[0].addr == 'ipc:///tmp/test.pipe'
    assert node.tubes[0].is_server
    assert node.tubes[0].tube_type_name == 'REP'
    assert node.tubes[0].identity == 'XXX'
    assert node.tubes[0].utf8_decoding
    assert node.get_tube_by_topic('foo/aaa') is not None
    assert node.get_tube_by_topic('xxx/bar') is not None
    assert node.get_tube_by_topic('xxx/aaa') is None


def test_load_schema_hierarchy():

    schema = """
    tubes:
    - name: tube1
      addr:  ipc:///tmp/test.pipe
      tube_type: REQ
      topics:
        - foo/#
        - +/bar
    - name: tube2
      addr:  ipc:///tmp/test.pipe
      tube_type: REQ
      utf8_decoding: False
      topics:
        - foo/test/#
        - +/bar/test
    """

    node = TubeNode(schema=yaml.safe_load(schema))

    assert len(node.tubes) == 2
    assert node.get_tube_by_topic('foo/aaa').name == 'tube1'
    assert node.get_tube_by_topic('foo/test/aaa').name == 'tube2'
    assert not node.get_tube_by_topic('foo/test/aaa').utf8_decoding
    assert node.get_tube_by_topic('xxx/bar').name == 'tube1'
    assert node.get_tube_by_topic('xxx/bar/test').name == 'tube2'
