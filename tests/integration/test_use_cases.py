import pytest

from datarobotx.idp.use_cases import get_or_create_use_case


@pytest.fixture
def cleanup_env(cleanup_dr):
    with cleanup_dr("useCases/"):
        yield


def test_get_or_create(dr_endpoint, dr_token, cleanup_env):
    use_case_id_1 = get_or_create_use_case(
        dr_endpoint,
        dr_token,
        "pytest use case #1",
    )
    assert len(use_case_id_1)

    use_case_id_2 = get_or_create_use_case(
        dr_endpoint,
        dr_token,
        "pytest use case #1",
    )
    assert use_case_id_1 == use_case_id_2

    use_case_id_3 = get_or_create_use_case(
        dr_endpoint,
        dr_token,
        "pytest use case #2",
    )
    assert use_case_id_1 != use_case_id_3
