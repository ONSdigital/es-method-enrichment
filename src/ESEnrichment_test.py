from . import ESEnrichment

def test_ESEnrichment():
    assert ESEnrichment.apply("Jane") == "hello Jane"
