from hypothesis import example, given, assume, strategies as st
from pytest import raises

from omop2graph.model import table_name


@given(s1=st.text(), s2=st.text())
def test_table_name(s1, s2):
    assume(s1 is not '')
    assume(s2 is not '')
    assert table_name(s1, schema=None) == s1
    assert table_name(s1, schema=s2) == f"{s2}.{s1}"

    with raises(ValueError):
        table_name('')
