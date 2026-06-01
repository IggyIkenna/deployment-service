"""Import-smoke tests for manifest library dependencies."""


def test_unified_api_contracts_importable():
    from unified_api_contracts import market

    assert hasattr(market, "CanonicalTicker")
