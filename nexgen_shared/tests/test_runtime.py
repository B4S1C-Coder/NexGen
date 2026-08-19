from nexgen_shared.runtime import mock_services_enabled


def test_mock_services_enabled_truthy_values() -> None:
    """Common truthy spellings of MOCK_SERVICES must enable fixture mode."""
    for value in ("1", "true", "TRUE", "yes", "on"):
        assert mock_services_enabled({"MOCK_SERVICES": value}) is True


def test_mock_services_enabled_defaults_false() -> None:
    """Missing or empty MOCK_SERVICES must keep live backends."""
    assert mock_services_enabled({}) is False
    assert mock_services_enabled({"MOCK_SERVICES": "false"}) is False
