import importlib
import json
import multiprocessing as mp
import os
import traceback
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

from orinoco.entities import ActionData


def _resolve_callable(dotted_path: str) -> Callable[..., Any]:
    module_path, _, attr = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError("Invalid dotted path: {}".format(dotted_path))
    module = importlib.import_module(module_path)
    return getattr(module, attr)


def _run_pipeline_sync(dotted_action: str, kwargs: Dict[str, Any]) -> ActionData:
    action_cls_or_instance = _resolve_callable(dotted_action)
    action = action_cls_or_instance if not isinstance(action_cls_or_instance, type) else action_cls_or_instance()
    return action.run_with_data(**kwargs)


def _process_target(dotted_action: str, kwargs: Dict[str, Any], result_queue: "mp.Queue[Tuple[bool, str]]") -> None:
    try:
        result: ActionData = _run_pipeline_sync(dotted_action, kwargs)
        result_queue.put((True, json.dumps(result.as_keyed_dict())))
    except Exception:
        result_queue.put((False, traceback.format_exc()))


@dataclass
class ExecutionResult:
    success: bool
    result: Optional[ActionData]
    error: Optional[str] = None
    job: Optional[Any] = None


@dataclass
class ProcessJob:
    process: mp.Process
    result_queue: "mp.Queue[Tuple[bool, str]]"


class Executor:
    def submit(self, dotted_action: str, wait: bool = True, **kwargs: Any) -> ExecutionResult:
        raise NotImplementedError


class ProcessExecutor(Executor):
    def __init__(self, start_method: Optional[str] = None, join_timeout: Optional[float] = None):
        self.start_method = start_method
        self.join_timeout = join_timeout

    def submit(self, dotted_action: str, wait: bool = True, **kwargs: Any) -> ExecutionResult:
        if self.start_method:
            try:
                mp.set_start_method(self.start_method, force=False)
            except RuntimeError:
                pass

        result_queue: "mp.Queue[Tuple[bool, str]]" = mp.Queue()  # type: ignore[type-arg]
        process = mp.Process(target=_process_target, args=(dotted_action, kwargs, result_queue))
        process.daemon = True
        process.start()
        if not wait:
            return ExecutionResult(success=True, result=None, job=ProcessJob(process=process, result_queue=result_queue))

        process.join(self.join_timeout)

        if process.is_alive():
            process.terminate()
            return ExecutionResult(success=False, result=None, error="Process timed out")

        success, payload = result_queue.get() if not result_queue.empty() else (False, "No result returned")
        if success:
            try:
                return ExecutionResult(success=True, result=ActionData.create(**json.loads(payload)))
            except Exception as err:
                return ExecutionResult(success=False, result=None, error=str(err))
        return ExecutionResult(success=False, result=None, error=payload)

    def collect(self, job: ProcessJob, timeout: Optional[float] = None) -> ExecutionResult:
        job.process.join(timeout if timeout is not None else self.join_timeout)
        if job.process.is_alive():
            return ExecutionResult(success=False, result=None, error="Process timed out", job=job)
        success, payload = job.result_queue.get() if not job.result_queue.empty() else (False, "No result returned")
        if success:
            try:
                return ExecutionResult(success=True, result=ActionData.create(**json.loads(payload)))
            except Exception as err:
                return ExecutionResult(success=False, result=None, error=str(err))
        return ExecutionResult(success=False, result=None, error=payload)

    def cancel(self, job: ProcessJob) -> bool:
        if job.process.is_alive():
            job.process.terminate()
            return True
        return False


class RedisRQExecutor(Executor):
    def __init__(self, queue_name: str = "default", connection: Any = None):
        try:
            import rq  # type: ignore
        except Exception as err:  # pragma: no cover - optional dependency
            raise RuntimeError("rq is not installed: {}".format(err))

        self.rq = rq
        self.queue = self.rq.Queue(queue_name, connection=connection)

    @staticmethod
    def rq_worker_entry(dotted_action: str, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        result = _run_pipeline_sync(dotted_action, kwargs)
        return result.as_keyed_dict()

    def submit(self, dotted_action: str, wait: bool = False, timeout: Optional[float] = None, **kwargs: Any) -> ExecutionResult:  # type: ignore[override]
        try:
            job = self.queue.enqueue(self.rq_worker_entry, dotted_action, kwargs, job_timeout=os.getenv("RQ_JOB_TIMEOUT"))
            if not wait:
                return ExecutionResult(success=True, result=None, job=job)
            result_dict = job.result if job.is_finished else job.result(timeout=timeout)  # type: ignore[attr-defined]
            if isinstance(result_dict, dict):
                return ExecutionResult(success=True, result=ActionData.create(**result_dict), job=job)
            return ExecutionResult(success=False, result=None, error="Unexpected result type", job=job)
        except Exception as err:  # pragma: no cover - external service
            return ExecutionResult(success=False, result=None, error=str(err))

    @staticmethod
    def collect(job: Any, timeout: Optional[float] = None) -> ExecutionResult:
        try:
            result_dict = job.result if job.is_finished else job.result(timeout=timeout)  # type: ignore[attr-defined]
            if isinstance(result_dict, dict):
                return ExecutionResult(success=True, result=ActionData.create(**result_dict), job=job)
            return ExecutionResult(success=False, result=None, error="Unexpected result type", job=job)
        except Exception as err:
            return ExecutionResult(success=False, result=None, error=str(err), job=job)

