class Task(object):

    dag = None

    def __init__(
        self,
        task_id = str,
        dag_id = str,
        
    ):
        self.task_id = task_id
        self.dag_id = dag_id
        self.depends = []

    def __repr__(self):
        return f"<Task: {self.task_id}>"

    def set_downstream(self, *tasks):
        """
        set the down stream tasks
        :param tasks: task objects
        :return:
        """
        for task in tasks:
            task.depends.append(self.task_id)


    def set_upstream(self, *tasks):
        """
        set the upstream tasks
        :param tasks: task objects
        :return:
        """
        for task in tasks:
            self.depends.append(task.task_id)


    def to_json(self):
        """
        convert Task object to dict
        :return:
        """

        r = {self.task_id: 
            {
                "id": self.task_id,
                "depends": self.depends,
            }
        }

        return r