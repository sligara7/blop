"""
Queueserver integration for running optimization through a Bluesky queueserver.

This module provides components for running optimization loops remotely through
a queueserver, rather than directly through a RunEngine.
"""

import logging
import threading
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from bluesky.callbacks import CallbackBase
from bluesky_queueserver_api import BPlan
from event_model import RunStart, RunStop

from .plans import default_acquire
from .protocols import ID_KEY, CanRegisterSuggestions, QueueserverOptimizationProblem

# ZMQ imports are deferred — only needed when using ZMQ transport
try:
    from bluesky.callbacks.zmq import RemoteDispatcher
except ImportError:
    RemoteDispatcher = None

logger = logging.getLogger("blop")


class HTTPManagerAPI:
    """Lightweight HTTP client for the Experiment Execution Service.

    Implements the same method signatures as
    ``bluesky_queueserver_api.zmq.REManagerAPI`` so it can be used as a
    drop-in replacement in ``QueueserverClient``.

    Parameters
    ----------
    http_server_uri : str
        Base URL of the EE service, e.g. ``"http://localhost:8001"``.
    """

    def __init__(self, http_server_uri: str):
        import httpx
        self._base = http_server_uri.rstrip("/")
        self._client = httpx.Client(base_url=self._base, timeout=30.0)

    def status(self, **kwargs) -> dict:
        r = self._client.get("/api/v1/status")
        r.raise_for_status()
        data = r.json()
        # Map EE fields to queueserver-compatible keys
        data.setdefault("worker_environment_exists", data.get("environment_initialized", False))
        return data

    def devices_allowed(self, **kwargs) -> dict:
        r = self._client.get("/api/v1/devices/allowed")
        r.raise_for_status()
        data = r.json()
        # Normalize to queueserver format: {"devices_allowed": {"name": {...}, ...}}
        devices = data.get("devices_allowed", data.get("devices", data))
        if isinstance(devices, list):
            data["devices_allowed"] = {(d["name"] if isinstance(d, dict) else d): {} for d in devices}
        return data

    def plans_allowed(self, **kwargs) -> dict:
        r = self._client.get("/api/v1/plans/allowed")
        r.raise_for_status()
        data = r.json()
        # Normalize to queueserver format: {"plans_allowed": {"name": {...}, ...}}
        plans = data.get("plans_allowed", data.get("plans", data))
        if isinstance(plans, list):
            data["plans_allowed"] = {(p["name"] if isinstance(p, dict) else p): {} for p in plans}
        return data

    def item_add(self, plan, **kwargs) -> dict:
        plan_dict = plan.to_dict() if hasattr(plan, "to_dict") else plan
        parameters = dict(plan_dict.get("kwargs", {}))
        args = plan_dict.get("args", [])
        if args:
            parameters["args"] = args
        payload = {
            "plan_name": plan_dict.get("name", ""),
            "parameters": parameters,
            "metadata": parameters.pop("md", {}),
        }
        r = self._client.post("/api/v1/queue/submit", json=payload)
        r.raise_for_status()
        return r.json()

    def queue_start(self, **kwargs) -> dict:
        r = self._client.post("/api/v1/queue/start")
        r.raise_for_status()
        return r.json()

    def wait_for_idle_or_paused(self, timeout: int = 600, **kwargs) -> None:
        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = self.status()
            state = status.get("manager_state", status.get("queue_state", "idle"))
            if state in ("idle", "paused"):
                return
            time.sleep(1.0)
        raise TimeoutError(f"Queue did not become idle within {timeout}s")

    def history_get(self, **kwargs) -> dict:
        r = self._client.get("/api/v1/history")
        r.raise_for_status()
        data = r.json()
        if "items" not in data:
            data = {"items": data if isinstance(data, list) else []}
        return data

    def close(self) -> None:
        self._client.close()


DEFAULT_ACQUIRE_PLAN_NAME: str = default_acquire.__name__
CORRELATION_UID_KEY: Literal["blop_correlation_uid"] = "blop_correlation_uid"


class ConsumerCallback(CallbackBase):
    """
    A callback that caches the start document and invokes a callback on stop.

    Parameters
    ----------
    callback : callable
        Function to call when a stop document is received.
        Signature: callback(start_doc, stop_doc)
    """

    def __init__(self, callback: Callable[[RunStart, RunStop], None] | None = None):
        super().__init__()
        self._start_doc_cache: RunStart | None = None
        self._callback = callback

    def start(self, doc: RunStart) -> None:
        """Caches the start document if it came from Blop"""
        if doc.get(CORRELATION_UID_KEY, None):
            self._start_doc_cache = doc
        else:
            self._start_doc_cache = None

    def stop(self, doc: RunStop) -> None:
        """Executes the callback if the cached start and stop document match"""
        if self._callback is not None and self._start_doc_cache is not None and self._start_doc_cache["uid"] == doc.get("run_start", doc.get("uid")):
            self._callback(self._start_doc_cache, doc)
        self._start_doc_cache = None


class QueueserverClient:
    """
    Handles communication with a Bluesky queueserver or Experiment Execution Service.

    Supports two transports:

    - **ZMQ** (traditional): Uses ``bluesky_queueserver_api.zmq.REManagerAPI``
      and a ZMQ ``RemoteDispatcher`` for document events.
    - **HTTP** (new): Uses ``bluesky_queueserver_api.http.REManagerAPI``
      and polls queue status for completion (no ZMQ required).

    Parameters
    ----------
    re_manager_api
        Manager API instance — either ZMQ or HTTP ``REManagerAPI``.
    zmq_consumer_addr : str, optional
        ZMQ document consumer address (e.g., ``"localhost:5578"``).
        Required for ZMQ transport; omit for HTTP.
    """

    def __init__(
        self,
        re_manager_api,
        zmq_consumer_addr: str | None = None,
    ):
        self._zmq_consumer_addr = zmq_consumer_addr
        self._use_http = zmq_consumer_addr is None

        self._rm = re_manager_api
        self._dispatcher = None
        self._consumer_callback: ConsumerCallback | None = None
        self._listener_thread: threading.Thread | None = None
        self._polling_thread: threading.Thread | None = None
        self._polling_stop_event: threading.Event | None = None

    def check_environment(self) -> None:
        """
        Verify that the queueserver environment is ready.

        Raises
        ------
        RuntimeError
            If the queueserver environment is not open.
        """
        status = self._rm.status()
        if status is None or not status.get("worker_environment_exists", False):
            raise RuntimeError("The queueserver environment is not open")

    def check_devices_available(self, device_names: Sequence[str]) -> None:
        """
        Verify that all specified devices are available in the queueserver.

        Parameters
        ----------
        device_names : Sequence[str]
            Names of devices to check.

        Raises
        ------
        ValueError
            If any device is not available.
        """
        res = self._rm.devices_allowed()
        allowed = res["devices_allowed"]
        for name in device_names:
            if name not in allowed:
                raise ValueError(f"Device '{name}' is not available in the queueserver environment")

    def check_plan_available(self, plan_name: str) -> None:
        """
        Verify that a plan is available in the queueserver.

        Parameters
        ----------
        plan_name : str
            Name of the plan to check.

        Raises
        ------
        ValueError
            If the plan is not available.
        """
        res = self._rm.plans_allowed()
        if plan_name not in res["plans_allowed"]:
            raise ValueError(f"Plan '{plan_name}' is not available in the queueserver environment")

    def submit_plan(self, plan: BPlan, autostart: bool = True, timeout: int = 600) -> None:
        """
        Submit a plan to the queueserver queue.

        Parameters
        ----------
        plan : BPlan
            The plan to submit.
        autostart : bool, optional
            If True, start the queue after adding the plan.
        timeout : float, optional
            Timeout in seconds when waiting for queue to be idle.
        """
        response = self._rm.item_add(plan)
        logger.debug(f"Submitted plan to queue. Response: {response}")

        if autostart:
            logger.debug("Waiting for queue to be idle or paused")
            self._rm.wait_for_idle_or_paused(timeout=timeout)
            response = self._rm.queue_start()
            logger.debug(f"Started queue. Response: {response}")

    def start_listener(self, on_stop: Callable[[RunStart, RunStop], None]) -> None:
        """
        Start listening for document events.

        For ZMQ transport, uses a ``RemoteDispatcher`` thread.
        For HTTP transport, polls ``status()`` for queue idle transitions
        and synthesises minimal start/stop documents so the agent's
        ``_on_acquisition_complete`` callback still fires.

        Parameters
        ----------
        on_stop : callable
            Callback invoked when a stop document is received.
            Signature: on_stop(start_doc, stop_doc)
        """
        if self._use_http:
            self._start_http_listener(on_stop)
        else:
            self._start_zmq_listener(on_stop)

    def _start_zmq_listener(self, on_stop: Callable[[RunStart, RunStop], None]) -> None:
        """Start ZMQ document listener."""
        if self._listener_thread is not None:
            logger.warning("Listener already running")
            return

        if RemoteDispatcher is None:
            raise ImportError("ZMQ transport requires bluesky[zmq]: pip install bluesky[zmq]")

        dispatcher = RemoteDispatcher(self._zmq_consumer_addr)
        self._consumer_callback = ConsumerCallback(callback=on_stop)
        dispatcher.subscribe(self._consumer_callback)

        logger.info("Starting ZMQ listener thread")
        self._listener_thread = threading.Thread(
            target=dispatcher.start,
            name="qserver-zmq-consumer",
            daemon=True,
        )
        self._listener_thread.start()
        self._dispatcher = dispatcher

    def _start_http_listener(self, on_stop: Callable[[RunStart, RunStop], None]) -> None:
        """Start HTTP polling listener.

        Polls the manager ``status()`` for queue idle transitions.  When
        the queue goes from running → idle we know a plan finished, and
        we fire ``on_stop`` with synthesised documents so the agent
        callback chain works unchanged.
        """
        if self._polling_thread is not None:
            logger.warning("HTTP listener already running")
            return

        stop_event = threading.Event()
        self._polling_stop_event = stop_event
        self._consumer_callback = ConsumerCallback(callback=on_stop)
        consumer_cb = self._consumer_callback
        rm = self._rm

        def _poll():
            was_running = False
            while not stop_event.is_set():
                try:
                    status = rm.status()
                    state = status.get("worker_environment_state", status.get("manager_state", ""))
                    is_running = state in ("executing_queue", "executing_task")

                    if was_running and not is_running:
                        # Queue just went idle — a plan finished.
                        try:
                            history = rm.history_get()
                            items = history.get("items", [])
                            if items:
                                last = items[-1]
                                # EE service: run_uid is top-level; queueserver: nested in result
                                uid = (
                                    last.get("run_uid")
                                    or last.get("result", {}).get("run_uids", [""])[0]
                                    or str(uuid.uuid4())
                                )
                                md = last.get("metadata", last.get("kwargs", {}).get("md", {}))
                            else:
                                uid = str(uuid.uuid4())
                                md = {}
                        except Exception:
                            uid = str(uuid.uuid4())
                            md = {}

                        start_doc = {"uid": uid, **md}
                        stop_doc = {"uid": str(uuid.uuid4()), "run_start": uid, "exit_status": "success"}
                        consumer_cb.start(start_doc)
                        consumer_cb.stop(stop_doc)

                    was_running = is_running
                except Exception as exc:
                    logger.debug(f"HTTP polling error: {exc}")

                stop_event.wait(timeout=1.0)

        logger.info("Starting HTTP polling listener thread")
        self._polling_thread = threading.Thread(
            target=_poll,
            name="qserver-http-poller",
            daemon=True,
        )
        self._polling_thread.start()

    def stop_listener(self) -> None:
        """Stop the listener (ZMQ or HTTP)."""
        if self._dispatcher is not None:
            self._dispatcher.stop()
            self._dispatcher = None
        if self._polling_stop_event is not None:
            self._polling_stop_event.set()
            if self._polling_thread is not None:
                self._polling_thread.join(timeout=5.0)
            self._polling_stop_event = None
            self._polling_thread = None
        self._consumer_callback = None
        self._listener_thread = None
        logger.info("Stopped listener")


@dataclass
class _OptimizationState:
    """Internal mutable state for an optimization run."""

    max_iterations: int = 1
    num_points: int = 1
    current_iteration: int = 0
    current_suggestions: list[dict] = field(default_factory=list)
    current_uid: str | None = None
    finished: bool = False


class QueueserverOptimizationRunner:
    """
    Runs optimization loops through a Bluesky queueserver.

    This class coordinates the optimization workflow by getting suggestions from
    the optimizer, submitting acquisition plans to the queueserver, and ingesting
    results when plans complete.

    Parameters
    ----------
    optimization_problem : QueueserverOptimizationProblem
        The optimization problem to solve, containing the optimizer, actuators,
        sensors, and evaluation function.
    queueserver_client : QueueserverClient
        Client for communicating with the queueserver.
    acquisition_plan_name : str
        Name of the acquisition plan registered in the queueserver.
    """

    def __init__(
        self,
        optimization_problem: QueueserverOptimizationProblem,
        queueserver_client: QueueserverClient,
    ):
        self._problem = optimization_problem
        self._client = queueserver_client
        self._plan_name = optimization_problem.acquisition_plan or DEFAULT_ACQUIRE_PLAN_NAME
        self._state: _OptimizationState | None = None
        self._continuous = True
        self._autostart = True

    @property
    def optimization_problem(self) -> QueueserverOptimizationProblem:
        """The optimization problem being solved."""
        return self._problem

    @property
    def is_running(self) -> bool:
        """Whether an optimization run is currently in progress."""
        return self._state is not None and not self._state.finished

    @property
    def current_iteration(self) -> int:
        """The current iteration number (0 if not running)."""
        return self._state.current_iteration if self._state else 0

    def run(self, iterations: int = 1, num_points: int = 1) -> None:
        """
        Start the optimization loop.

        Validates the queueserver state, then begins the suggest -> acquire -> ingest
        cycle. This method returns immediately; the optimization runs asynchronously
        via callbacks.

        Parameters
        ----------
        iterations : int
            Number of optimization iterations to run.
        num_points : int
            Number of points to suggest per iteration.

        Raises
        ------
        RuntimeError
            If the queueserver environment is not ready.
        ValueError
            If required devices or plans are not available.
        """
        # TODO: What if there is already a run in-progress?
        self._validate()
        self._state = _OptimizationState(max_iterations=iterations, num_points=num_points)
        self._continuous = True
        self._client.start_listener(on_stop=self._on_acquisition_complete)
        self._submit_next()

    def submit_suggestions(self, suggestions: list[dict]) -> None:
        """
        Manually submit suggestions to the queue. This method returns immediately; the
        optimization runs asynchronously via callbacks.

        Parameters
        ----------
        suggestions : list[dict]
            Parameter combinations to evaluate. Can be:

            - Optimizer suggestions (with "_id" keys from suggest())
            - Manual points (without "_id", requires CanRegisterSuggestions protocol)
        """
        self._validate()
        self._state = _OptimizationState(max_iterations=1, num_points=len(suggestions))
        self._continuous = False
        self._client.start_listener(on_stop=self._on_acquisition_complete)
        self._submit_next_manual(suggestions)

    def stop(self) -> None:
        """
        Stop the optimization loop gracefully.

        The current acquisition will complete, but no further iterations will run.
        """
        self._continuous = False
        self._client.stop_listener()
        if self._state is not None:
            self._state.finished = True
        logger.info("Optimization stopped")

    def _validate(self) -> None:
        """Validate queueserver environment, devices, and plan availability."""
        self._client.check_environment()

        # Collect device names from actuators and sensors
        actuator_names = list(self._problem.actuators)
        sensor_names = list(self._problem.sensors)
        self._client.check_devices_available(actuator_names + sensor_names)

        self._client.check_plan_available(self._plan_name)

    def _submit_next_manual(self, suggestions: list[dict]) -> None:
        """Get suggestions from optimizer and submit plan to queueserver."""
        if self._state is None:
            raise RuntimeError("_submit_next called before run()")

        # Ensure the suggestions have an ID_KEY or register them with the optimizer
        if not isinstance(self.optimization_problem.optimizer, CanRegisterSuggestions) and any(
            ID_KEY not in suggestion for suggestion in suggestions
        ):
            raise ValueError(
                f"All suggestions must contain an '{ID_KEY}' key to later match with the outcomes or your optimizer must "
                "implement the `blop.protocols.CanRegisterSuggestions` protocol. Please review your optimizer "
                f"implementation. Got suggestions: {suggestions}"
            )
        elif isinstance(self.optimization_problem.optimizer, CanRegisterSuggestions):
            suggestions = self.optimization_problem.optimizer.register_suggestions(suggestions)

        self._state.current_iteration += 1
        self._state.current_suggestions = suggestions
        self._state.current_uid = str(uuid.uuid4())

        logger.info(f"Submitting manually specified suggestion(s) with correlation uid: {self._state.current_uid}")

        plan = self._build_plan()
        self._client.submit_plan(plan, autostart=self._autostart)

    def _submit_next(self) -> None:
        """Get suggestions from optimizer and submit plan to queueserver."""
        if self._state is None:
            raise RuntimeError("_submit_next called before run()")
        self._state.current_iteration += 1
        self._state.current_suggestions = self._problem.optimizer.suggest(self._state.num_points)
        self._state.current_uid = str(uuid.uuid4())

        logger.info(
            f"Submitting iteration {self._state.current_iteration}/{self._state.max_iterations} "
            f"with correlation uid: {self._state.current_uid}"
        )

        plan = self._build_plan()
        self._client.submit_plan(plan, autostart=self._autostart)

    def _build_plan(self) -> BPlan:
        """Construct a BPlan from the current suggestions."""
        if self._state is None:
            raise RuntimeError("_build_plan called before run()")
        # Build metadata
        md: dict[str, Any] = {
            CORRELATION_UID_KEY: self._state.current_uid,
            "blop_suggestions": self._state.current_suggestions,
        }

        return BPlan(
            self._plan_name,
            self._state.current_suggestions,
            list(self._problem.actuators),
            list(self._problem.sensors),
            md=md,
        )

    def _on_acquisition_complete(self, start_doc: RunStart, stop_doc: RunStop) -> None:
        """Callback when acquisition finishes. Ingest results and maybe continue."""
        if self._state is None:
            raise RuntimeError("_on_acquisition_complete called before run()")
        if self._state.current_uid is None:
            raise RuntimeError("current_uid not set")
        if self._state.current_uid != start_doc.get("blop_correlation_uid", None):
            raise RuntimeError(
                "current_uid did not match start document. "
                f"Got: {start_doc.get('blop_correlation_uid', None)}, Expected: {self._state.current_uid}"
            )
        logger.info(f"Acquisition complete for uid: {self._state.current_uid}")

        # Evaluate the results
        outcomes = self._problem.evaluation_function(
            uid=start_doc["uid"],
            suggestions=self._state.current_suggestions,
        )

        logger.info(f"Evaluated {len(outcomes)} outcomes")

        # Ingest into optimizer
        self._problem.optimizer.ingest(outcomes)

        # Continue if appropriate
        if self._continuous and self._state.current_iteration < self._state.max_iterations:
            logger.info("Continuing to next iteration")
            self._submit_next()
        else:
            self._state.finished = True
            self._client.stop_listener()
            logger.info(f"Optimization complete after {self._state.current_iteration} iterations")


def create_http_client(
    http_server_uri: str,
    *,
    api_key: str | None = None,
    use_ee_service: bool = False,
) -> QueueserverClient:
    """Create a QueueserverClient that communicates over HTTP.

    Supports two backends:

    - **bluesky-httpserver** (default): The standard queueserver HTTP
      frontend.  Uses ``bluesky_queueserver_api.http.REManagerAPI``.
      Requires ``pip install bluesky-queueserver-api``.
    - **Experiment Execution Service**: The new Bluesky remote
      architecture service (SVC-001).  Set ``use_ee_service=True``.

    Parameters
    ----------
    http_server_uri : str
        Base URL, e.g. ``"https://xf31id1-tst-qs1.nsls2.bnl.gov"``
        or ``"http://localhost:8001"``.
    api_key : str, optional
        Single-user API key for bluesky-httpserver authentication.
    use_ee_service : bool
        If True, use the built-in ``HTTPManagerAPI`` adapter for the
        Experiment Execution Service.  If False (default), use
        ``bluesky_queueserver_api.http.REManagerAPI``.

    Returns
    -------
    QueueserverClient
        A client ready for use with ``QueueserverAgent``.

    Examples
    --------
    Connect to bluesky-httpserver:

    >>> client = create_http_client(
    ...     "https://xf31id1-tst-qs1.nsls2.bnl.gov",
    ...     api_key="01cc...",
    ... )

    Connect to EE service:

    >>> client = create_http_client(
    ...     "http://localhost:8001",
    ...     use_ee_service=True,
    ... )
    """
    if use_ee_service:
        rm = HTTPManagerAPI(http_server_uri=http_server_uri)
    else:
        try:
            from bluesky_queueserver_api.http import REManagerAPI
        except ImportError:
            raise ImportError(
                "bluesky-httpserver transport requires bluesky-queueserver-api: "
                "pip install bluesky-queueserver-api"
            )
        rm = REManagerAPI(http_server_uri=http_server_uri)
        if api_key:
            rm.set_authorization_key(api_key=api_key)
    return QueueserverClient(re_manager_api=rm)
