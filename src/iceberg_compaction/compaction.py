from dataclasses import dataclass

from pyiceberg.table import Table

from iceberg_compaction.commit import Committer
from iceberg_compaction.execution import Execution, ExecutionMetrics
from iceberg_compaction.planning import PlanMetrics, Planner


@dataclass(frozen=True)
class CompactionResult:
    snapshot_id: int | None
    plan_metrics: PlanMetrics
    execution_metrics: ExecutionMetrics


def compact(table: Table, planner: Planner, execution: Execution) -> CompactionResult:
    plan = planner.plan(table)
    result = execution.execute(plan)
    snapshot_id = Committer().commit(result)
    table.refresh()
    return CompactionResult(snapshot_id, plan.metrics, result.metrics)
