from datalineup_engine.core.topic import TopicMessage
from datalineup_engine.utils.options import asdict
from datalineup_engine.utils.options import fromdict


def test_save_load_topic_message() -> None:
    m = TopicMessage(
        args={
            "hey": "ho",
            "none": None,
        },
        tags={"tag": "tag"},
        metadata={"a": {"foo": "bar"}, "b": {"foo": None}},
    )
    m_loaded = fromdict(asdict(m), TopicMessage)
    assert m_loaded.args["hey"] == "ho"
    assert m_loaded.args["none"] is None
    assert m_loaded.tags["tag"] == "tag"
    assert m_loaded.metadata["a"] == {"foo": "bar"}
    assert m_loaded.metadata["b"] == {"foo": None}
