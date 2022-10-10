from biflow import DAG
from biflow import Task

d = DAG('dag-One')

t = Task('task1')


d.add_task(t)

d.tree_view()