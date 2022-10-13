import copy
from collections import OrderedDict
import logging
import json
from datetime import datetime, timedelta
import functools
from biflow.exceptions import TaskNotFound, DuplicateTaskIdFound
from typing import Iterable

LOG = logging.getLogger(__name__)

class DAG(object):
    """
    A dag (directed acyclic graph) is a collection of tasks with directional
    dependencies. The DAG needs to run each individual tasks as their depend
    encies are met. Certain tasks have the property of depending on their ow
    n past, meaning that they can't rununtil their previous schedule (and up
    stream tasks) are completed.

    DAGs essentially act as namespaces for tasks. A task_id can only be
    added once to a DAG.
    """

    _comps = {
        'dag_id',
        'task_ids',
        'parent_dag',
    }

    parent_dag = None  # Gets set when DAGs are loaded
    
    def __init__(
        self,
        dag_id: str,
        description: str = None,
    ):


        # check self.params and convert them into ParamsDict
        self._dag_id = dag_id
        self._description = description
        self.task_dict = {}
    
    def __repr__(self):
        return f"<DAG: {self.dag_id}>"

    def __eq__(self, other):
        if type(self) == type(other):
            # Use getattr() instead of __dict__ as __dict__ doesn't return
            # correct values for properties.
            return all(getattr(self, c, None) == getattr(other, c, None) for c in self._comps)
        return False

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return self.dag_id < other.dag_id

    @property
    def dag_id(self) -> str:
        return self._dag_id

    @property
    def is_subdag(self) -> bool:
        return self.parent_dag is not None

    @property
    def description(self) -> None:
        return self._description

    @property
    def roots(self) -> list:
        """Return nodes with no parents. These are first to execute and are called roots or root nodes."""
        return [task for task in self.tasks if not task.upstream_list]

 
    @property
    def tasks(self) -> list:
        return list(self.task_dict.values())

    @tasks.setter
    def tasks(self, val):
        raise AttributeError('DAG.tasks can not be modified. Use dag.add_task() instead.')

    @property
    def task_ids(self) -> list[str]:
        return list(self.task_dict.keys())

    def has_task(self, task_id: str):
        return task_id in self.task_dict


    def get_task(self, task_id: str, include_subdags: bool = False):
        if task_id in self.task_dict:
            return self.task_dict[task_id]
        if include_subdags:
            for dag in self.subdags:
                if task_id in dag.task_dict:
                    return dag.task_dict[task_id]
        raise TaskNotFound(f"Task {task_id} not found")


    def add_task(self, task) -> None:
        """
        Add a task to the DAG
        :param task: the task you want to add
        """


        task_id = task.task_id

        if task_id in self.task_dict and self.task_dict[task_id] is not task:
            raise DuplicateTaskIdFound(f"Task id '{task_id}' has already been added to the DAG")
        else:
            self.task_dict[task_id] = task
            task.dag = self

        self.task_count = len(self.task_dict)


    def add_tasks(self, tasks: Iterable) -> None:
        """
        Add a list of tasks to the DAG
        :param tasks: a lit of tasks you want to add
        """
        for task in tasks:
            self.add_task(task)

    def to_json(self):
        jsn = OrderedDict()
        for task in self.tasks:
            jsn.update(task.to_json())
        print(json.dumps(jsn, indent=4))
