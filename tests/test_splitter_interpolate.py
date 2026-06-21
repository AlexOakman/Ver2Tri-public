from core.splitter import SQLSplitter
from core.state_manager import StateManager


def test_splitter_marks_right_side_as_interpolate_source(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    splitter = SQLSplitter(state_manager)

    sql = """
create local temp table user_is_ct on commit preserve rows as (
    select user_id, act_date, user_is_ct
    from analytics_src.source_table
)
order by user_id
segmented by hash(user_id) all nodes;

create local temp table consumer_table on commit preserve rows as (
    select acd.user_id
    from acd_for_user_details acd
    left join user_is_ct uic
        on acd.user_id = uic.user_id
       and acd.event_date interpolate previous value uic.act_date
)
order by user_id
segmented by hash(user_id) all nodes;
"""

    parts = splitter.split(sql)

    source_part = parts[0]
    consumer_part = parts[1]

    assert source_part["table_name"] == "user_is_ct"
    assert consumer_part["table_name"] == "consumer_table"
    assert consumer_part["interpolate_sources"] == [
        {
            "source_part": 0,
            "source_table": "USER_IS_CT",
            "consumer_part": 1,
        }
    ]
    assert source_part["interpolate_consumers"] == [
        {
            "consumer_part": 1,
            "consumer_table": "USER_IS_CT",
        }
    ]


def test_splitter_skips_analyze_statistics_and_noop_semicolon_parts(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    splitter = SQLSplitter(state_manager)

    parts = splitter.split(
        """
select analyze_statistics('tmp_table');
;
select 1 as value;
"""
    )

    assert len(parts) == 1
    assert parts[0]["content"] == "select 1 as value;"
