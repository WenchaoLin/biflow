class Task(object):

    dag = None

    def __init__(
        self,
        task_id = str,
        dag_id = str,
        
    ):
        self.task_id = task_id
        self.dag_id = dag_id
        self.upstream_list = []
        self.downstream_list = []

    def __repr__(self):
        return f"<Task: {self.task_id}>"

    def set_downstream(self, *tasks):
        """
        set the down stream tasks
        :param tasks: task objects
        :return:
        """
        for task in tasks:
            task.downstream_list.append(self.task_id)


    def set_upstream(self, *tasks):
        """
        set the upstream tasks
        :param tasks: task objects
        :return:
        """
        for task in tasks:
            self.upstream_list.append(task.task_id)


    def to_json(self):
        """
        convert Task object to dict
        :return:
        """

        r = {self.task_id: 
            {
                "id": self.task_id,
                "depends": self.upstream_list,
            }
        }

        return r